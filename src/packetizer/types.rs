use crate::prelude::*;

use std::{
    fmt::Display,
    net::{Ipv4Addr, SocketAddrV4},
};

use crate::packet_processor::serialize::{PacketDeserialize, PacketSerialize};
use aes_gcm_siv::aead::generic_array::sequence::Concat;
use derive_more::Display;
use tokio::time::Instant;
use ubass_macros::{PacketDeserialize, PacketSerialize};

use super::fingerprint::{Fingerprint, HeaderSerialize};

pub const MAX_PAYLOAD_LENGTH: usize = 1400;

pub enum PacketWrapper {
    HelloPacket(HelloPacket),
    TrackRequestPacket(TrackRequestPacket),
    DataPacket(DataPacket),
    MetadataPacket(MetadataPacket),
    ParityPacket(ParityPacket),
    AckPacket(AckPacket),
    RetransmitPacket(RetransmitPacket),
}

#[repr(C)]
#[derive(HeaderSerialize, PacketSerialize, PacketDeserialize)]
pub struct HelloPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub timestamp: Timestamp,
    pub proposed_session_id: SessionId,
    pub public_key: PublicKey,
    pub app_id: AppId,
    pub host_address: SocketAddrV4,
}

impl HelloPacket {
    pub fn new(
        opts: Options,
        proposed_session_id: SessionId,
        public_key: PublicKey,
        app_id: AppId,
        host_address: SocketAddrV4,
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Host;
        let control_type = ControlType::Host(HostControlType::Hello);
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            timestamp,
            proposed_session_id,
            public_key,
            app_id,
            host_address,
        }
    }
}

#[repr(C)]
#[derive(HeaderSerialize, PacketSerialize)]
pub struct TrackRequestPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub timestamp: Timestamp,
    pub session_id: SessionId,
    pub payload: Vec<u8>,
}

impl TrackRequestPacket {
    #[must_use]
    pub fn request_track(opts: Options, session_id: SessionId, payload: Vec<u8>) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.add(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::TrackRequest);
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            timestamp,
            session_id,
            payload,
        }
    }

    #[must_use]
    pub fn request_metadata(opts: Options, session_id: SessionId, payload: Vec<u8>) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.add(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::MetadataRequest);
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            timestamp,
            session_id,
            payload,
        }
    }
}

#[repr(C)]
#[derive(HeaderSerialize, PacketDeserialize, PacketSerialize)]
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
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Data;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            batch_id,
            fec_info,
            session_id,
            timestamp,
            byte_range_start,
            payload,
        }
    }
}

#[derive(HeaderSerialize, PacketDeserialize, PacketSerialize)]
#[repr(C)]
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
    ) -> Self {
        debug_assert!(
            position.0 < buffer_size.0,
            "Invariant broken while constructing `MetadataPacket`: \
            position is laregr than buffer size ({position} > {buffer_size})"
        );

        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Metadata;
        let timestamp = Timestamp::now();

        Self {
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
        }
    }
}

#[repr(C)]
#[derive(HeaderSerialize, PacketDeserialize, PacketSerialize)]
pub struct ParityPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: Vec<u8>,
}

impl ParityPacket {
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH + 4;
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<Vec<u8>>();
    pub const MIN_SIZE: usize = Self::HEADER_SIZE + 9;

    #[must_use]
    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        payload: Vec<u8>,
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let packet_type_batch_id = PacketTypeFecBatchID {
            packet_type: PacketType::Parity,
            batch_id,
        };
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type_batch_id,
            fec_info,
            session_id,
            timestamp,
            payload,
        }
    }
}

#[repr(C)]
#[derive(HeaderSerialize, PacketDeserialize, PacketSerialize)]
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
    fn new(opts: Options, session_id: SessionId, playback_type: PlaybackControlType) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.add(OptionFlags::RequireAck);
        let packet_type = PacketType::Playback;
        let control_type = playback_type.into();
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            session_id,
            timestamp,
        }
    }

    pub fn play(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Play)
    }

    pub fn pause(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Pause)
    }

    pub fn stop(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Stop)
    }
}

#[repr(C)]
#[derive(HeaderSerialize, PacketDeserialize, PacketSerialize)]
pub struct AckPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    reserved: Reserved<1>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub fingerprint: PacketFingerprint,
}

impl AckPacket {
    pub const HEADER_SIZE: usize = size_of::<AckPacket>();
    pub const MIN_SIZE: usize = AckPacket::HEADER_SIZE;

    pub fn new(opts: Options, session_id: SessionId, fingerprint: PacketFingerprint) -> Self {
        debug_assert!(
            !opts.contains(OptionFlags::RequireAck),
            "Invariant broken while constructing `AckPacket`: \
            flag `RequireAck` was present, which should not be allowed."
        );

        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Ack;
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            reserved,
            session_id,
            timestamp,
            fingerprint,
        }
    }
}

#[repr(C)]
#[derive(HeaderSerialize, PacketSerialize, PacketDeserialize)]
pub struct RetransmitPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub buffer_id: Option<BufferId>,
    pub timestamp: Timestamp,
    pub session_id: SessionId,
    pub payload: Vec<ByteRange>,
}

impl RetransmitPacket {
    // closest I can get to `MAX_PAYLOAD_LENGTH` while aligning to 6 bytes
    const LOCAL_MAX_PAYLOAD_LENGTH: usize = 1398;
    const HEADER_SIZE: usize = size_of::<Self>() - Self::LOCAL_MAX_PAYLOAD_LENGTH;

    pub fn new(
        opts: Options,
        buffer_id: Option<BufferId>,
        session_id: SessionId,
        payload: Vec<ByteRange>,
    ) -> Self {
        debug_assert!(
            payload.len() <= (Self::LOCAL_MAX_PAYLOAD_LENGTH / size_of::<ByteRange>()),
            "Invariant broken while constructing a `RetransmitPacket`: payload bigger than allowed max size: {} `ByteRange`s ({} bytes) > {} `ByteRange`s ({} bytes)",
            payload.len(),
            (payload.len() * size_of::<ByteRange>()),
            (Self::LOCAL_MAX_PAYLOAD_LENGTH / size_of::<ByteRange>()),
            Self::LOCAL_MAX_PAYLOAD_LENGTH
        );

        let version = Version::CURRENT_VERSION;
        let opts = opts.add(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::Retransmit);
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            buffer_id,
            timestamp,
            session_id,
            payload,
        }
    }
}

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(C)]
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

    pub fn new(opts: Options, session_id: SessionId) -> Self {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Error;
        let error_type = ErrorType::SessionDoesNotExist;
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            error_type,
            reserved,
            session_id,
            timestamp,
        }
    }
}

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(C)]
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
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Error;
        let error_type = if incomprehensible {
            ErrorType::IncomprehensiblePacket
        } else {
            ErrorType::UnexpectedPacket
        };
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
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
        }
    }

    pub fn unexpected(
        opts: Options,
        session_id: SessionId,
        received_packet_type: PacketType,
        received_secondary_type: SecondaryType,
        received_fingerprint: PacketFingerprint,
    ) -> Self {
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
    ) -> Self {
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
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.add(OptionFlags::RequireAck);
        let packet_type = PacketType::Error;
        let error_type = ErrorType::AppReject;
        let reserved = Reserved;
        let timestamp = Timestamp::now();
        let payload = message.into_bytes();

        Self {
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
        }
    }
}

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(C)]
pub struct IncompatibleVersion {
    pub zero_version: Version,
    pub min_version: Version,
}

impl IncompatibleVersion {
    pub const HEADER_SIZE: usize = size_of::<Self>();
    pub fn packet() -> [u8; Self::HEADER_SIZE] {
        let mut buffer = [0u8; Self::HEADER_SIZE];
        Self {
            zero_version: Version::new(0, 0, 0),
            min_version: Version::MIN_COMPATIBLE_VERSION,
        }
        .serialize(&mut buffer);
        buffer
    }
}

#[derive(PacketDeserialize, PacketSerialize)]
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

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(transparent)]
pub struct PacketFingerprint([u8; 16]);

impl<T: Fingerprint> From<T> for PacketFingerprint {
    fn from(value: T) -> Self {
        Self(value.fingerprint())
    }
}

#[derive(PacketDeserialize, PacketSerialize, PartialEq, Default)]
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

#[derive(PacketDeserialize, PacketSerialize, Display)]
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

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(transparent)]
pub struct AppId(pub u64);

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(transparent)]
pub struct PublicKey(pub [u8; 32]);

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy, Display)]
#[repr(transparent)]
pub struct BytePosition(pub u32);

#[derive(PacketDeserialize, PacketSerialize)]
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    fn now() -> Self {
        #[allow(clippy::cast_possible_truncation)]
        Self(
            Instant::now()
                .duration_since(*PROTOCOL_EPOCH.get().expect(
                    "Invariant broken while constructing `Timestamp`: \
        `PROTOCOL_EPOCH` is not initialized",
                ))
                .as_millis() as u64,
        )
    }
}

pub struct Reserved<const N: usize>;

impl<const N: usize> PacketSerialize for Reserved<N> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N {
            false
        } else {
            buf[..N].fill(0);
            true
        }
    }

    fn sized(&self) -> usize {
        N
    }
}

impl<const N: usize> PacketDeserialize for Reserved<N> {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < N || bytes[..N].iter().any(|n| *n != 0) {
            None
        } else {
            Some(Reserved)
        }
    }
}

#[derive(
    PacketDeserialize, PacketSerialize, Debug, Clone, Copy, Eq, PartialOrd, Ord, PartialEq,
)]
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
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (major, minor, patch) = self.parse();
        write!(f, "{major}.{minor}.{patch}")
    }
}

/// Enum of all possible packet types as of now
#[derive(PacketDeserialize, PacketSerialize, Clone, Copy, PartialEq, Debug)]
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

impl PacketSerialize for ControlType {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        match self {
            ControlType::Host(packet) => packet.serialize(buf),
            ControlType::Session(packet) => packet.serialize(buf),
            ControlType::Playback(packet) => packet.serialize(buf),
        }
    }

    fn sized(&self) -> usize {
        1
    }
}

impl PacketDeserialize for ControlType {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if let Some(control_type) = HostControlType::deserialize(bytes) {
            Some(Self::Host(control_type))
        } else if let Some(control_type) = SessionControlType::deserialize(bytes) {
            Some(Self::Session(control_type))
        } else {
            PlaybackControlType::deserialize(bytes).map(Self::Playback)
        }
    }
}

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy, PartialEq)]
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

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy, PartialEq)]
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

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy, PartialEq)]
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

#[derive(PacketSerialize, PacketDeserialize, Clone, Copy)]
#[repr(u8)]
pub enum ErrorType {
    AppReject = 1,
    UnexpectedPacket,
    IncomprehensiblePacket,
    SessionDoesNotExist,
}

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BatchID(pub u16);

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy)]
#[repr(C)]
pub struct PacketTypeFecBatchID {
    pub packet_type: PacketType,
    pub batch_id: BatchID,
}

#[derive(PacketDeserialize, PacketSerialize, Debug, PartialEq)]
#[repr(transparent)]
pub struct Options(u16);

impl Options {
    #[inline]
    pub fn construct(flags: &[OptionFlags]) -> Self {
        Self(
            flags
                .iter()
                .map(|x| *x as u16)
                .reduce(|f1, f2| f1 | f2)
                .unwrap_or(0),
        )
    }

    #[inline]
    pub const fn contains(&self, flag: OptionFlags) -> bool {
        self.0 & (flag as u16) != 0
    }

    #[must_use]
    pub const fn remove(mut self, flag: OptionFlags) -> Self {
        self.0 &= !(flag as u16);
        self
    }

    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub const fn add(mut self, flag: OptionFlags) -> Self {
        self.0 |= flag as u16;
        self
    }

    pub fn deconstruct(&self) -> Vec<OptionFlags> {
        OptionFlags::VARIANTS
            .iter()
            .copied()
            .filter(|e| (*e as u16) & self.0 != 0)
            .collect()
    }
}

#[derive(Clone, Copy, Display)]
#[repr(u8)]
#[variants_array]
pub enum OptionFlags {
    RequireAck = 1 << 0,
    Metadata = 1 << 1,
}

#[repr(C)]
#[derive(PacketSerialize, PacketDeserialize)]
struct Key(u128, u128);

#[repr(transparent)]
#[derive(PacketDeserialize, PacketSerialize, Debug, PartialEq, Eq, Hash, Clone, Copy, Display)]
pub struct SessionId(pub u64);

#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Clone, Copy, Debug, PartialEq)]
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
#[derive(Debug, PacketDeserialize, PacketSerialize)]
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

impl PacketSerialize for Vec<ByteRange> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.len() * size_of::<ByteRange>() {
            false
        } else {
            self.iter()
                .enumerate()
                .all(|(i, e)| e.serialize(&mut buf[i * size_of::<ByteRange>()..]))
        }
    }

    fn sized(&self) -> usize {
        self.len() * size_of::<ByteRange>()
    }
}

impl PacketDeserialize for Vec<ByteRange> {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        const SIZE: usize = size_of::<ByteRange>();
        if bytes.len() < SIZE {
            None
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

            Some(result)
        }
    }
}
