use crate::{
    manager::{OutboundSender, state::Port},
    prelude::*,
};

use core::ops::Deref;
use std::{fmt::Display, net::SocketAddr};

use crate::packet_processor::serialize::Serialize;
use async_trait::async_trait;
use derive_more::{Deref, Display};
use rand::Rng;

use crate::manager::AppId;
use crate::packet_processor::fingerprint::{Fingerprint, Headers, Payload};

pub const MAX_PAYLOAD_LENGTH: usize = 1384;

pub struct PacketWrapper {
    pub addr: SocketAddr,
    pub packet: Packet,
}

#[derive(Clone)]
pub enum Packet {
    HelloPacket(Box<HelloPacket>),
    TrackRequestPacket(Box<TrackRequestPacket>),
    DataPacket(Box<DataPacket>),
    MetadataPacket(Box<MetadataPacket>),
    ParityPacket(Box<ParityPacket>),
    AckPacket(Box<AckPacket>),
    RetransmitPacket(Box<RetransmitPacket>),
    PlaybackStatusPacket(Box<PlaybackStatusPacket>),
    IncompatibleVersionPacket(Box<IncompatibleVersionPacket>),
    SessionDoesNotExistErrorPacket(Box<SessionDoesNotExistErrorPacket>),
    UnexpectedPacketErrorPacket(Box<UnexpectedPacketErrorPacket>),
    AppRejectErrorPacket(Box<AppRejectErrorPacket>),
}

impl Packet {
    pub fn wrap(self, addr: SocketAddr) -> PacketWrapper {
        PacketWrapper { addr, packet: self }
    }
}

impl TryFrom<&Packet> for PacketFingerprint {
    type Error = ();

    fn try_from(value: &Packet) -> core::result::Result<Self, ()> {
        Ok(match value {
            Packet::TrackRequestPacket(packet) => packet.as_ref().into(),
            Packet::DataPacket(packet) => packet.as_ref().into(),
            Packet::MetadataPacket(packet) => packet.as_ref().into(),
            Packet::ParityPacket(packet) => packet.as_ref().into(),
            Packet::RetransmitPacket(packet) => packet.as_ref().into(),
            Packet::PlaybackStatusPacket(packet) => packet.as_ref().into(),
            Packet::UnexpectedPacketErrorPacket(packet) => packet.as_ref().into(),
            Packet::AppRejectErrorPacket(packet) => packet.as_ref().into(),
            _ => return Err(()),
        })
    }
}

#[derive(SendPacket, Clone, Serialize, Headers)]
pub struct HelloPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub proposed_session_id: SessionId,
    pub timestamp: Timestamp,
    pub public_key: PublicKey,
    pub receiving_port: Port,
    pub app_id: AppId,
}

impl HelloPacket {
    pub fn new(
        opts: Options,
        proposed_session_id: SessionId,
        public_key: PublicKey,
        app_id: AppId,
        receiving_port: Port,
    ) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Host;
        let control_type = ControlType::Host(HostControlType::Hello);
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            proposed_session_id,
            timestamp,
            public_key,
            receiving_port,
            app_id,
        })
    }
}

#[derive(SendPacket, Headers, Payload, Serialize, Clone)]
pub struct TrackRequestPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: Vec<u8>,
}

impl TrackRequestPacket {
    #[must_use]
    pub fn request_track(opts: Options, session_id: SessionId, payload: Vec<u8>) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::TrackRequest);
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            session_id,
            timestamp,
            payload,
        })
    }

    #[must_use]
    pub fn request_metadata(opts: Options, session_id: SessionId, payload: Vec<u8>) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::MetadataRequest);
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            session_id,
            timestamp,
            payload,
        })
    }
}

#[derive(SendPacket, Headers, Payload, Serialize, Clone)]
pub struct DataPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub batch_id: BatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub byte_range_start: BytePosition,
    pub payload: Vec<u8>,
}

impl DataPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<Vec<u8>>();
    pub const MIN_SIZE: usize = Self::HEADER_SIZE + 1;

    #[must_use]
    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        byte_range_start: BytePosition,
        payload: Vec<u8>,
    ) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Data;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            batch_id,
            fec_info,
            session_id,
            timestamp,
            byte_range_start,
            payload,
        })
    }
}

#[derive(SendPacket, Clone, Headers, Payload, Serialize)]
pub struct MetadataPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub batch_id: BatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub buffer_id: BufferId,
    pub buffer_size: BufferSize,
    pub position: BytePosition,
    pub payload: Vec<u8>,
}

impl MetadataPacket {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        buffer_id: BufferId,
        buffer_size: BufferSize,
        position: BytePosition,
        payload: Vec<u8>,
    ) -> Box<Self> {
        debug_assert!(
            position.0 < buffer_size.0,
            "Invariant broken while constructing `MetadataPacket`: \
            position is laregr than buffer size ({position} > {buffer_size})"
        );

        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Metadata;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            batch_id,
            fec_info,
            session_id,
            timestamp,
            buffer_id,
            buffer_size,
            position,
            payload,
        })
    }
}

#[derive(SendPacket, Clone, Headers, Payload, Serialize)]
pub struct ParityPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub batch_id: BatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: Vec<u8>,
}

impl ParityPacket {
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH + size_of::<BytePosition>();
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<Vec<u8>>();
    pub const MIN_SIZE: usize = Self::HEADER_SIZE + size_of::<BytePosition>() + 1;

    #[must_use]
    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        payload: Vec<u8>,
    ) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Parity;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            batch_id,
            fec_info,
            session_id,
            timestamp,
            payload,
        })
    }
}

#[derive(SendPacket, Clone, Serialize, Headers)]
pub struct PlaybackStatusPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
}

impl PlaybackStatusPacket {
    #[inline]
    fn new(opts: Options, session_id: SessionId, playback_type: PlaybackControlType) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Playback;
        let control_type = playback_type.into();
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            session_id,
            timestamp,
        })
    }

    #[inline]
    pub fn play(opts: Options, session_id: SessionId) -> Box<Self> {
        Self::new(opts, session_id, PlaybackControlType::Play)
    }

    #[inline]
    pub fn pause(opts: Options, session_id: SessionId) -> Box<Self> {
        Self::new(opts, session_id, PlaybackControlType::Pause)
    }

    #[inline]
    pub fn stop(opts: Options, session_id: SessionId) -> Box<Self> {
        Self::new(opts, session_id, PlaybackControlType::Stop)
    }
}

#[derive(SendPacket, Clone, Serialize)]
pub struct AckPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    reserved: Reserved<3>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub fingerprint: PacketFingerprint,
}

impl AckPacket {
    pub const HEADER_SIZE: usize = size_of::<AckPacket>();
    pub const MIN_SIZE: usize = AckPacket::HEADER_SIZE;

    pub fn new(opts: Options, session_id: SessionId, fingerprint: PacketFingerprint) -> Box<Self> {
        debug_assert!(
            !opts.contains(OptionFlags::RequireAck),
            "Invariant broken while constructing `AckPacket`: \
            flag `RequireAck` is present, but an ack packet must never be acked"
        );

        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Ack;
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            reserved,
            session_id,
            timestamp,
            fingerprint,
        })
    }
}

#[derive(SendPacket, Headers, Serialize, Clone)]
pub struct RetransmitPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub buffer_id: Option<BufferId>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: Vec<ByteRange>,
}

impl RetransmitPacket {
    // closest I can get to `MAX_PAYLOAD_LENGTH` while aligning to 6 bytes
    const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH - (MAX_PAYLOAD_LENGTH % 6);
    const HEADER_SIZE: usize = size_of::<Self>() - Self::LOCAL_MAX_PAYLOAD_LENGTH;

    pub fn new(
        opts: Options,
        buffer_id: Option<BufferId>,
        session_id: SessionId,
        payload: Vec<ByteRange>,
    ) -> Box<Self> {
        debug_assert!(
            payload.len() <= (Self::LOCAL_MAX_PAYLOAD_LENGTH / size_of::<ByteRange>()),
            "Invariant broken while constructing a `RetransmitPacket`: payload bigger than allowed max size: {} `ByteRange`s ({} bytes) > {} `ByteRange`s ({} bytes)",
            payload.len(),
            (payload.len() * size_of::<ByteRange>()),
            (Self::LOCAL_MAX_PAYLOAD_LENGTH / size_of::<ByteRange>()),
            Self::LOCAL_MAX_PAYLOAD_LENGTH
        );

        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::Retransmit);
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            control_type,
            buffer_id,
            session_id,
            timestamp,
            payload,
        })
    }
}

#[derive(SendPacket, Clone, Serialize)]
pub struct SessionDoesNotExistErrorPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub error_type: ErrorType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
}

impl SessionDoesNotExistErrorPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>();

    pub fn new(opts: Options, session_id: SessionId) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Error;
        let error_type = ErrorType::SessionDoesNotExist;
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            error_type,
            reserved,
            session_id,
            timestamp,
        })
    }
}

#[derive(SendPacket, Clone, Serialize, Headers)]
pub struct UnexpectedPacketErrorPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub error_type: ErrorType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub received_packet_type: PacketType,
    pub received_secondary_type: SecondaryType,
    pub received_fingerprint: PacketFingerprint,
}

impl UnexpectedPacketErrorPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>();

    fn new(
        opts: Options,
        session_id: SessionId,
        received_packet_type: PacketType,
        received_secondary_type: SecondaryType,
        received_fingerprint: PacketFingerprint,
        incomprehensible: bool,
    ) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Error;
        let error_type = if incomprehensible {
            ErrorType::IncomprehensiblePacket
        } else {
            ErrorType::UnexpectedPacket
        };
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            error_type,
            reserved,
            session_id,
            timestamp,
            received_packet_type,
            received_secondary_type,
            received_fingerprint,
        })
    }

    pub fn unexpected(
        opts: Options,
        session_id: SessionId,
        received_packet_type: PacketType,
        received_secondary_type: SecondaryType,
        received_fingerprint: PacketFingerprint,
    ) -> Box<Self> {
        Self::new(
            opts,
            session_id,
            received_packet_type,
            received_secondary_type,
            received_fingerprint,
            false,
        )
    }

    pub fn incomprehensible(
        opts: Options,
        session_id: SessionId,
        received_packet_type: PacketType,
        received_secondary_type: SecondaryType,
        received_fingerprint: PacketFingerprint,
    ) -> Box<Self> {
        Self::new(
            opts,
            session_id,
            received_packet_type,
            received_secondary_type,
            received_fingerprint,
            true,
        )
    }
}

#[derive(SendPacket, Clone, Serialize, Headers, Payload)]
pub struct AppRejectErrorPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub error_type: ErrorType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub received_type: PacketType,
    pub received_control_type: ControlType,
    pub received_fingerprint: PacketFingerprint,
    pub payload: Vec<u8>,
}

impl AppRejectErrorPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<Vec<u8>>();

    pub fn new(
        opts: Options,
        session_id: SessionId,
        received_type: PacketType,
        received_control_type: ControlType,
        received_fingerprint: PacketFingerprint,
        message: String,
    ) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Error;
        let error_type = ErrorType::AppReject;
        let reserved = Reserved;
        let timestamp = Timestamp::now();
        let payload = message.into_bytes();

        Box::new(Self {
            version,
            opts,
            packet_type,
            error_type,
            reserved,
            session_id,
            timestamp,
            received_type,
            received_control_type,
            received_fingerprint,
            payload,
        })
    }
}

#[derive(SendPacket, Clone, Copy, Serialize)]
pub struct IncompatibleVersionPacket {
    pub zero_version: Version,
    pub min_version: Version,
}

impl IncompatibleVersionPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>();
    pub fn packet() -> Box<Self> {
        Box::new(Self {
            zero_version: Version::new(0, 0, 0),
            min_version: Version::MIN_COMPATIBLE_VERSION,
        })
    }
}

#[derive(Clone, Copy, Serialize)]
#[repr(transparent)]
pub struct SecondaryType([u8; 2]);

impl From<ControlType> for SecondaryType {
    fn from(value: ControlType) -> Self {
        let mut buf = [0u8; 2];
        value.serialize(&mut buf);
        Self(buf)
    }
}

impl From<ErrorType> for SecondaryType {
    fn from(value: ErrorType) -> Self {
        let mut buf = [0u8; 2];
        value.serialize(&mut buf);
        Self(buf)
    }
}

#[derive(Clone, Serialize, Hash, PartialEq, Eq)]
#[repr(transparent)]
pub struct PacketFingerprint([u8; 16]);

impl<T: Fingerprint> From<&T> for PacketFingerprint {
    fn from(value: &T) -> Self {
        Self(value.fingerprint())
    }
}

#[derive(Serialize, PartialEq, Default, Clone, Copy)]
#[repr(transparent)]
pub struct BufferId(u16);

impl BufferId {
    pub fn new(id: u16) -> Self {
        debug_assert!(
            id != 0,
            "Invariant broken while constructing `BufferId`: \
            a buffer ID can never be 0"
        );

        Self(id)
    }
}

#[derive(Clone, Copy, Serialize, Display)]
#[repr(transparent)]
pub struct BufferSize(u32);

impl BufferSize {
    const MAX_MB: usize = 10;
    const MAX_BUFFER_SIZE: usize = Self::MAX_MB * 1024 * 1024;
    pub fn new(size: u32) -> Self {
        debug_assert!(
            (size as usize) < Self::MAX_BUFFER_SIZE,
            "Invariant broken while constructing `BufferSize`: \
            provided size larger than allowed ({} > {})",
            size,
            Self::MAX_MB
        );

        Self(size)
    }
}

#[derive(Serialize, Clone, Copy)]
#[repr(transparent)]
pub struct PublicKey([u8; 32]);

impl From<x25519_dalek::PublicKey> for PublicKey {
    fn from(value: x25519_dalek::PublicKey) -> Self {
        PublicKey(value.to_bytes())
    }
}

impl From<PublicKey> for x25519_dalek::PublicKey {
    fn from(value: PublicKey) -> Self {
        x25519_dalek::PublicKey::from(*value)
    }
}

impl Deref for PublicKey {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Serialize, Debug, Clone, Copy, Display)]
#[repr(transparent)]
pub struct BytePosition(pub u32);

#[derive(Clone, Copy)]
pub struct Reserved<const N: usize>;

impl<const N: usize> Serialize for Reserved<N> {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < N {
            Err(())
        } else {
            buf[..N].fill(0);
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        if bytes.len() < N {
            Err(())
        } else {
            Ok(Reserved)
        }
    }

    fn sized(&self) -> usize {
        N
    }
}

#[derive(Serialize, Debug, Clone, Copy, Eq, PartialOrd, Ord, PartialEq)]
#[repr(transparent)]
pub struct Version(u16);

impl Version {
    pub const CURRENT_VERSION: Version = Version::new(0, 0, 1);
    pub const MIN_COMPATIBLE_VERSION: Version = Version::new(0, 0, 1);

    #[inline]
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self((major as u16) << 12 | (minor as u16) << 8 | patch as u16)
    }

    #[inline]
    pub const fn parse(&self) -> (u8, u8, u8) {
        (
            (self.0 >> 12) as u8,
            ((self.0 >> 8) & 0xF) as u8,
            (self.0 & 0xFF) as u8,
        )
    }

    #[inline]
    pub fn is_compatible(&self) -> bool {
        *self >= Version::MIN_COMPATIBLE_VERSION
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (major, minor, patch) = self.parse();
        write!(f, "{major}.{minor}.{patch}")
    }
}

/// Enum of all possible packet types as of now
#[derive(Serialize, Clone, Copy, PartialEq, Debug)]
#[repr(u8)]
pub enum PacketType {
    Data = 1,
    Metadata = 2,
    Parity = 3,
    Ack = 4,
    Host = 7,
    Session = 8,
    Playback = 9,
    Error = 10,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ControlType {
    Host(HostControlType),
    Session(SessionControlType),
    Playback(PlaybackControlType),
}

impl Serialize for ControlType {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        match self {
            ControlType::Host(packet) => packet.serialize(buf),
            ControlType::Session(packet) => packet.serialize(buf),
            ControlType::Playback(packet) => packet.serialize(buf),
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        if let Ok(control_type) = HostControlType::deserialize(bytes) {
            Ok(Self::Host(control_type))
        } else if let Ok(control_type) = SessionControlType::deserialize(bytes) {
            Ok(Self::Session(control_type))
        } else {
            PlaybackControlType::deserialize(bytes).map(Self::Playback)
        }
    }

    fn sized(&self) -> usize {
        1
    }
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum HostControlType {
    Hello = 201,
}

impl From<HostControlType> for ControlType {
    #[inline]
    fn from(value: HostControlType) -> Self {
        Self::Host(value)
    }
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum SessionControlType {
    Retransmit = 1,
    TrackRequest = 2,
    MetadataRequest = 3,
}

impl From<SessionControlType> for ControlType {
    #[inline]
    fn from(value: SessionControlType) -> Self {
        Self::Session(value)
    }
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PlaybackControlType {
    Play = 101,
    Pause = 102,
    Stop = 103,
}

impl From<PlaybackControlType> for ControlType {
    #[inline]
    fn from(value: PlaybackControlType) -> Self {
        Self::Playback(value)
    }
}

#[derive(Serialize, Clone, Copy)]
#[repr(u8)]
pub enum ErrorType {
    AppReject = 1,
    UnexpectedPacket,
    IncomprehensiblePacket,
    SessionDoesNotExist,
}

#[derive(Display, Deref, Serialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BatchID(u16);

impl BatchID {
    pub fn new(id: u16) -> Self {
        debug_assert!(
            id != 0,
            "Invariant broken while constructing `BatchID`:\
        ID must never be 0."
        );

        Self(id)
    }
}

#[derive(Flags, Serialize, Debug, PartialEq, Clone, Copy)]
#[repr(transparent)]
#[flagtype(OptionFlags)]
pub struct Options(u16);

#[derive(Clone, Copy, Display)]
#[repr(u8)]
#[variants_array]
pub enum OptionFlags {
    RequireAck = 1 << 0,
    Metadata = 1 << 1,
}

#[repr(transparent)]
#[derive(Serialize, Deref, Debug, PartialEq, Eq, Hash, Clone, Copy, Display)]
pub struct SessionId(u64);

impl SessionId {
    #[inline]
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        Self(rng.next_u64())
    }
}

#[repr(C)]
#[derive(Serialize, Clone, Copy, Debug, PartialEq)]
pub struct FECInfo {
    pub batch_size: u8,
    pub batch_pos: u8,
    pub recovery_count: u8,
}

impl FECInfo {
    pub fn new(batch_size: u8, batch_pos: u8, recovery_size: u8) -> Self {
        debug_assert!(
            batch_pos < batch_size,
            "Invariant broken while constructing `FECInfo`: \
            `batch_pos` is bigger than `batch_size` ({batch_pos} >= {batch_size})"
        );
        debug_assert!(
            recovery_size <= batch_size,
            "Invariant broken while constructing `FECInfo`: \
            there are more recovery shards than there are data shards ({recovery_size} > {batch_size})"
        );
        Self {
            batch_size,
            batch_pos,
            recovery_count: recovery_size,
        }
    }
}

#[repr(C)]
#[derive(Debug, Serialize, Clone)]
pub struct ByteRange {
    start: BytePosition,
    length: u16,
}

impl ByteRange {
    pub fn new(start: BytePosition, length: u16) -> Self {
        debug_assert!(
            length as usize <= MAX_PAYLOAD_LENGTH,
            "Invariant broken while constructing a `ByteRange`:\
            `length` is too big ({length}). To combine multiple continous ranges, use `Self::concat()`"
        );
        Self { start, length }
    }

    pub fn concat(&mut self, other: &ByteRange) -> bool {
        debug_assert!(
            self.start.0 + self.length as u32 == other.start.0
                || other.start.0 + other.length as u32 == self.start.0,
            "Invariant broken while trying to concatincate two `ByteRange`s: The two are not continous. \
            self: {self:?}, other: {other:?}"
        );

        if let Some(res) = self.length.checked_add(other.length) {
            self.length = res;
            if other.start.0 + other.length as u32 == self.start.0 {
                self.start = other.start;
            }
            true
        } else {
            false
        }
    }
}

impl Serialize for Vec<ByteRange> {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.len() * size_of::<ByteRange>() {
            Err(())
        } else {
            for (i, e) in self.iter().enumerate() {
                e.serialize(&mut buf[i * size_of::<ByteRange>()..])?;
            }
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        const SIZE: usize = size_of::<ByteRange>();
        if bytes.len() < SIZE {
            Err(())
        } else {
            let mut buf = vec![];
            let mut result = vec![];
            for byte in bytes {
                if buf.len() >= SIZE {
                    result.push(ByteRange::deserialize(&buf)?);
                    buf.clear();
                }
                buf.push(*byte);
            }

            Ok(result)
        }
    }

    fn sized(&self) -> usize {
        self.len() * size_of::<ByteRange>()
    }
}
