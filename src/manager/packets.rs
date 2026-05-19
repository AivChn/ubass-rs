#![allow(clippy::unnecessary_box_returns)]
use crate::{
    manager::{
        ManagerToProcessor,
        state::{HandshakeId, Port},
    },
    prelude::*,
};

use core::ops::Deref;
use std::{fmt::Display, net::SocketAddr, vec};

use crate::packet_processor::serialize::Serialize;
use async_trait::async_trait;
use derive_more::{Deref, DerefMut, Display};

use crate::manager::AppId;
use crate::packet_processor::fingerprint::{Fingerprint, Headers, Payload};

pub const MAX_PAYLOAD_LENGTH: usize = 1384;

#[derive(Debug, Clone)]
pub struct PacketWrapper {
    pub addr: SocketAddr,
    pub packet: Packet,
}

#[derive(Clone, Debug)]
pub enum Packet {
    HelloPacket(Box<HelloPacket>),
    TrackRequestPacket(Box<TrackRequestPacket>),
    DataPacket(Box<DataPacket>),
    MetadataPacket(Box<MetadataPacket>),
    ParityPacket(Box<ParityPacket>),
    AckPacket(Box<AckPacket>),
    KeepAlivePacket(Box<KeepAlivePacket>),
    HandshakeAckPacket(Box<HandshakeAckPacket>),
    RetransmitPacket(Box<RetransmitPacket>),
    PlaybackControlPacket(Box<PlaybackControlPacket>),
    IncompatibleVersionPacket(Box<IncompatibleVersionPacket>),
    SessionDoesNotExistErrorPacket(Box<SessionDoesNotExistErrorPacket>),
    UnexpectedPacketErrorPacket(Box<UnexpectedPacketErrorPacket>),
    TrackRejectionPacket(Box<TrackRejectionPacket>),
    CloseSessionPacket(Box<CloseSessionPacket>),
    HandshakeRejection(Box<HandshakeRejection>),
}

impl Packet {
    #[must_use]
    pub fn wrap(self, addr: SocketAddr) -> PacketWrapper {
        PacketWrapper { addr, packet: self }
    }

    #[must_use]
    pub fn session_id(&self) -> Option<SessionId> {
        match self {
            Packet::HelloPacket(packet) => Some(packet.proposed_session_id),
            Packet::TrackRequestPacket(packet) => Some(packet.session_id),
            Packet::DataPacket(packet) => Some(packet.session_id),
            Packet::MetadataPacket(packet) => Some(packet.session_id),
            Packet::ParityPacket(packet) => Some(packet.session_id),
            Packet::AckPacket(packet) => Some(packet.session_id),
            Packet::RetransmitPacket(packet) => Some(packet.session_id),
            Packet::PlaybackControlPacket(packet) => Some(packet.session_id),
            Packet::SessionDoesNotExistErrorPacket(packet) => Some(packet.session_id),
            Packet::UnexpectedPacketErrorPacket(packet) => Some(packet.session_id),
            Packet::TrackRejectionPacket(packet) => Some(packet.session_id),
            Packet::CloseSessionPacket(packet) => Some(packet.session_id),
            Packet::HandshakeRejection(packet) => Some(packet.session_id),
            Packet::KeepAlivePacket(packet) => Some(packet.session_id),
            Packet::IncompatibleVersionPacket(_) | Packet::HandshakeAckPacket(_) => {
                debug_assert!(
                    false,
                    "Invariant broken while trying to get the session_id of a packet: \
                        tried to get the session ID of the `IncompatibleVersionPacket` packet, which does not have one"
                );
                None
            }
        }
    }

    pub fn mark_resend(&mut self) {
        match self {
            Packet::HelloPacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::TrackRequestPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::DataPacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::MetadataPacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::ParityPacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::AckPacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::KeepAlivePacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::HandshakeAckPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::RetransmitPacket(packet) => packet.opts = packet.opts.set(OptionFlags::Resend),
            Packet::PlaybackControlPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::SessionDoesNotExistErrorPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::UnexpectedPacketErrorPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::TrackRejectionPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::CloseSessionPacket(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::HandshakeRejection(packet) => {
                packet.opts = packet.opts.set(OptionFlags::Resend);
            }
            Packet::IncompatibleVersionPacket(_) => {}
        }
    }
}

impl SendPacket for Packet {
    type Sender = ManagerToProcessor;

    #[allow(
        mismatched_lifetime_syntaxes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn send<'async_trait>(
        self: Box<Self>,
        sender: Self::Sender,
        address: SocketAddr,
    ) -> ::core::pin::Pin<
        Box<dyn ::core::future::Future<Output = ()> + ::core::marker::Send + 'async_trait>,
    >
    where
        Self: 'async_trait,
    {
        match *self {
            Packet::HelloPacket(packet) => packet.send(sender, address),
            Packet::TrackRequestPacket(packet) => packet.send(sender, address),
            Packet::DataPacket(packet) => packet.send(sender, address),
            Packet::MetadataPacket(packet) => packet.send(sender, address),
            Packet::ParityPacket(packet) => packet.send(sender, address),
            Packet::AckPacket(packet) => packet.send(sender, address),
            Packet::KeepAlivePacket(packet) => packet.send(sender, address),
            Packet::RetransmitPacket(packet) => packet.send(sender, address),
            Packet::PlaybackControlPacket(packet) => packet.send(sender, address),
            Packet::IncompatibleVersionPacket(packet) => packet.send(sender, address),
            Packet::SessionDoesNotExistErrorPacket(packet) => packet.send(sender, address),
            Packet::UnexpectedPacketErrorPacket(packet) => packet.send(sender, address),
            Packet::TrackRejectionPacket(packet) => packet.send(sender, address),
            Packet::HandshakeAckPacket(packet) => packet.send(sender, address),
            Packet::CloseSessionPacket(packet) => packet.send(sender, address),
            Packet::HandshakeRejection(packet) => packet.send(sender, address),
        }
    }
}

impl TryFrom<&Packet> for PacketFingerprint {
    type Error = ();

    fn try_from(value: &Packet) -> core::result::Result<Self, ()> {
        Ok(match value {
            Packet::HelloPacket(packet) => packet.as_ref().into(),
            Packet::TrackRequestPacket(packet) => packet.as_ref().into(),
            Packet::DataPacket(packet) => packet.as_ref().into(),
            Packet::MetadataPacket(packet) => packet.as_ref().into(),
            Packet::ParityPacket(packet) => packet.as_ref().into(),
            Packet::RetransmitPacket(packet) => packet.as_ref().into(),
            Packet::PlaybackControlPacket(packet) => packet.as_ref().into(),
            Packet::UnexpectedPacketErrorPacket(packet) => packet.as_ref().into(),
            Packet::TrackRejectionPacket(packet) => packet.as_ref().into(),
            Packet::CloseSessionPacket(packet) => packet.as_ref().into(),
            _x @ (Packet::AckPacket(_)
            | Packet::IncompatibleVersionPacket(_)
            | Packet::KeepAlivePacket(_)
            | Packet::SessionDoesNotExistErrorPacket(_)
            | Packet::HandshakeAckPacket(_)
            | Packet::HandshakeRejection(_)) => {
                return Err(());
            }
        })
    }
}

#[derive(Debug, SendPacket, Clone, Serialize, Headers)]
pub struct HelloPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub proposed_session_id: SessionId,
    pub handshake_id: HandshakeId,
    pub timestamp: Timestamp,
    pub public_key: PublicKey,
    pub receiving_port: Port,
    pub app_id: AppId,
}

impl HelloPacket {
    pub fn new(
        opts: Options,
        proposed_session_id: SessionId,
        handshake_id: HandshakeId,
        public_key: impl Into<PublicKey>,
        app_id: AppId,
        receiving_port: Port,
    ) -> Box<Self> {
        debug_assert!(
            *receiving_port > 1024,
            "Invariant broken while constructing a `HelloPacket`: \
                This hosts port is below 1024 ({receiving_port:?})"
        );
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "HelloPacket");

        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Host;
        let control_type = ControlType::Host(HostControlType::Hello);
        let public_key = public_key.into();
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Box::new(Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            proposed_session_id,
            handshake_id,
            timestamp,
            public_key,
            receiving_port,
            app_id,
        })
    }
}

#[derive(Debug, SendPacket, Headers, Payload, Serialize, Clone)]
pub struct TrackRequestPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: PayloadField,
}

impl TrackRequestPacket {
    #[must_use]
    pub fn request_track(opts: Options, session_id: SessionId, payload: PayloadField) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "TrackRequestPacket");
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
    pub fn request_metadata(
        opts: Options,
        session_id: SessionId,
        payload: impl Into<PayloadField>,
    ) -> Box<Self> {
        let payload = payload.into();

        debug_assert!(
            payload.len() < MAX_PAYLOAD_LENGTH,
            "Invariant broken while constructing `TrackRequestPacket`: \
                payload is larger than `MAX_PAYLOAD_LENGTH` ({} > {})",
            payload.len(),
            MAX_PAYLOAD_LENGTH
        );

        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "TrackRequestPacket");
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

#[derive(Debug, SendPacket, Headers, Payload, Serialize, Clone, PartialEq)]
pub struct DataPacket {
    pub version: Version,               // 2B
    pub opts: Options,                  // 2B
    pub packet_type: PacketType,        // 1B
    pub batch_id: BatchID,              // 2B
    pub fec_info: FECInfo,              // 3B
    pub session_id: SessionId,          // 8B
    pub timestamp: Timestamp,           // 8B
    pub byte_range_start: BytePosition, // 4B
    pub payload: PayloadField,          // 0..1384B
}

impl DataPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<PayloadField>();
    pub const MIN_SIZE: usize = Self::HEADER_SIZE + 1;

    #[must_use]
    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        byte_range_start: BytePosition,
        payload: impl Into<PayloadField>,
    ) -> Box<Self> {
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "DataPacket");
        let payload = payload.into();
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

#[derive(Debug, SendPacket, Clone, Headers, Payload, Serialize)]
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
    pub payload: PayloadField,
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
        payload: impl Into<PayloadField>,
    ) -> Box<Self> {
        debug_assert!(
            *position < *buffer_size,
            "Invariant broken while constructing `MetadataPacket`: \
            position is laregr than buffer size ({position} > {buffer_size})"
        );
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "MetadataPacket");

        let payload = payload.into();
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

#[derive(Debug, SendPacket, Clone, Headers, Payload, Serialize)]
pub struct ParityPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub batch_id: BatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: ParityPayload,
}

impl ParityPacket {
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize =
        MAX_PAYLOAD_LENGTH + size_of::<BytePosition>() + size_of::<u16>();
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<PayloadField>();
    pub const MIN_SIZE: usize = Self::HEADER_SIZE + size_of::<BytePosition>() + 1;

    #[must_use]
    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        payload: impl Into<ParityPayload>,
    ) -> Self {
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "ParityPacket");
        let version = Version::CURRENT_VERSION;
        let packet_type = PacketType::Parity;
        let timestamp = Timestamp::now();
        let payload = payload.into();

        Self {
            version,
            opts,
            packet_type,
            batch_id,
            fec_info,
            session_id,
            timestamp,
            payload,
        }
    }
}

#[derive(Debug, SendPacket, Clone, Serialize, Headers)]
pub struct PlaybackControlPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: PlaybackControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub seek_position: BytePosition,
}

impl PlaybackControlPacket {
    #[inline]
    #[must_use]
    pub fn new(
        opts: Options,
        session_id: SessionId,
        playback_type: PlaybackControlType,
        seek_pos: Option<BytePosition>,
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "PlaybackStatusPacket");
        let packet_type = PacketType::Playback;
        let control_type = playback_type;
        let reserved = Reserved;
        let timestamp = Timestamp::now();
        let seek_position = seek_pos.unwrap_or(BytePosition(0));

        Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            session_id,
            timestamp,
            seek_position,
        }
    }

    #[inline]
    #[must_use]
    pub fn play(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Play, None)
    }

    #[inline]
    #[must_use]
    pub fn pause(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Pause, None)
    }

    #[inline]
    #[must_use]
    pub fn close(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Close, None)
    }

    #[inline]
    #[must_use]
    pub fn done(opts: Options, session_id: SessionId) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Done, None)
    }

    #[inline]
    #[must_use]
    pub fn seek(opts: Options, session_id: SessionId, seek_pos: BytePosition) -> Self {
        Self::new(opts, session_id, PlaybackControlType::Done, Some(seek_pos))
    }
}

#[derive(Debug, SendPacket, Clone, Serialize)]
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

    #[must_use]
    pub fn new(opts: Options, session_id: SessionId, fingerprint: PacketFingerprint) -> Box<Self> {
        debug_assert!(
            !opts.contains(OptionFlags::RequireAck),
            "Invariant broken while constructing `AckPacket`: \
            flag `RequireAck` is present, but an ack packet must never be acked"
        );
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "AckPacket");

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

// fake impl just because im too lazy to special case KeepAlive
impl Serialize for Option<SocketAddr> {
    fn serialize(&self, _buf: &mut [u8]) -> EmptyResult {
        Ok(())
    }

    fn deserialize(_bytes: &[u8]) -> core::result::Result<Self, ()> {
        Ok(None)
    }

    fn sized(&self) -> usize {
        0
    }
}

#[derive(Debug, SendPacket, Clone, Serialize)]
pub struct KeepAlivePacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    reserved: Reserved<3>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub address: Option<SocketAddr>,
}

impl KeepAlivePacket {
    #[must_use]
    pub fn new(opts: Options, session_id: SessionId) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.unset(OptionFlags::RequireAck);
        let packet_type = PacketType::KeepAlive;
        let reserved = Reserved;
        let timestamp = Timestamp::now();
        let address = None;

        Self {
            version,
            opts,
            packet_type,
            reserved,
            session_id,
            timestamp,
            address,
        }
    }
}

#[derive(Debug, SendPacket, Clone, Serialize)]
pub struct HandshakeAckPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    reserved: Reserved<3>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub handshake_id: HandshakeId,
}

impl HandshakeAckPacket {
    #[must_use]
    pub fn new(session_id: SessionId, handshake_id: HandshakeId) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = Options::none();
        let packet_type = PacketType::HandshakeAck;
        let reserved = Reserved;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            reserved,
            session_id,
            timestamp,
            handshake_id,
        }
    }
}

#[derive(Debug, SendPacket, Headers, Serialize, Clone)]
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
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH - (MAX_PAYLOAD_LENGTH % 6);
    //const HEADER_SIZE: usize = size_of::<Self>() - Self::LOCAL_MAX_PAYLOAD_LENGTH;

    #[must_use]
    pub fn data(opts: Options, session_id: SessionId, payload: Vec<ByteRange>) -> Self {
        debug_assert!(
            payload.len() <= (Self::LOCAL_MAX_PAYLOAD_LENGTH / ByteRange::elem_size()),
            "Invariant broken while constructing a `RetransmitPacket`:\
                payload bigger than allowed max size: {} `ByteRange`s ({} bytes) > {} `ByteRange`s ({} bytes)",
            payload.len(),
            (payload.len() * ByteRange::elem_size()),
            (Self::LOCAL_MAX_PAYLOAD_LENGTH / ByteRange::elem_size()),
            Self::LOCAL_MAX_PAYLOAD_LENGTH
        );

        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "RetransmitPacket");
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::Retransmit);
        let buffer_id = None;
        let timestamp = Timestamp::now();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            buffer_id,
            session_id,
            timestamp,
            payload,
        }
    }

    #[must_use]
    pub fn metadata(
        opts: Options,
        session_id: SessionId,
        buffer_id: BufferId,
        payload: Vec<ByteRange>,
    ) -> Box<Self> {
        debug_assert!(
            payload.len() <= (Self::LOCAL_MAX_PAYLOAD_LENGTH / ByteRange::elem_size()),
            "Invariant broken while constructing a `RetransmitPacket`:\
                payload bigger than allowed max size: {} `ByteRange`s ({} bytes) > {} `ByteRange`s ({} bytes)",
            payload.len(),
            (payload.len() * ByteRange::elem_size()),
            (Self::LOCAL_MAX_PAYLOAD_LENGTH / ByteRange::elem_size()),
            Self::LOCAL_MAX_PAYLOAD_LENGTH
        );

        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let opts = opts.set(OptionFlags::Metadata);
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "RetransmitPacket");
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::Retransmit);
        let buffer_id = Some(buffer_id);
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

#[derive(Debug, SendPacket, Clone, Serialize, Headers)]
pub struct CloseSessionPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
}

impl CloseSessionPacket {
    #[must_use]
    pub fn new(opts: Options, session_id: SessionId) -> Box<Self> {
        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = ControlType::Session(SessionControlType::Close);
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
}

#[derive(Debug, Serialize, Clone, Copy)]
#[repr(u8)]
pub enum HandshakeRejectionReason {
    App = 1,
    IdCollision = 2,
}

#[derive(Debug, SendPacket, Clone, Serialize)]
pub struct HandshakeRejection {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub handshake_id: HandshakeId,
    pub reason: HandshakeRejectionReason,
    pub payload: PayloadField,
}

impl HandshakeRejection {
    #[must_use]
    pub fn new(
        opts: Options,
        session_id: SessionId,
        reason: HandshakeRejectionReason,
        handshake_id: HandshakeId,
        payload: impl Into<PayloadField>,
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let opts = opts.unset(OptionFlags::RequireAck);
        let packet_type = PacketType::Host;
        let control_type = ControlType::Host(HostControlType::HandshakeReject);
        let reserved = Reserved;
        let timestamp = Timestamp::now();
        let payload = payload.into();

        Self {
            version,
            opts,
            packet_type,
            control_type,
            reserved,
            session_id,
            timestamp,
            handshake_id,
            reason,
            payload,
        }
    }
}

#[derive(Debug, SendPacket, Clone, Serialize)]
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

    #[must_use]
    pub fn new(opts: Options, session_id: SessionId) -> Box<Self> {
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "SessionDoesNotExistErrorPacket");
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

#[derive(Debug, SendPacket, Clone, Serialize, Headers)]
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

    #[must_use]
    pub fn new(
        opts: Options,
        session_id: SessionId,
        received_packet_type: PacketType,
        received_secondary_type: SecondaryType,
        received_fingerprint: PacketFingerprint,
        incomprehensible: bool,
    ) -> Box<Self> {
        #[cfg(debug_assertions)]
        debug_assert!(
            valid_secondary_type(received_secondary_type),
            "Invariant broken while constructing `UnexpectedPacketErrorPacket`: \
                the value of the received secondary type was not an actual secondary type ({})",
            received_secondary_type.0
        );
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "UnexpectedPacketErrorPacket");

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

    #[must_use]
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

    #[must_use]
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

#[derive(Debug, SendPacket, Clone, Serialize, Headers, Payload)]
pub struct TrackRejectionPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: Reserved<2>,
    pub session_id: SessionId,
    pub timestamp: Timestamp,
    pub payload: PayloadField,
}

impl TrackRejectionPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>() - size_of::<PayloadField>();

    #[must_use]
    pub fn new(
        opts: Options,
        session_id: SessionId,
        track_id: impl Into<PayloadField>,
    ) -> Box<Self> {
        #[cfg(debug_assertions)]
        assert_opts_valid(opts, "TrackRejectionPacket");

        let version = Version::CURRENT_VERSION;
        let opts = opts.set(OptionFlags::RequireAck);
        let packet_type = PacketType::Session;
        let control_type = SessionControlType::TrackReject.into();
        let reserved = Reserved;
        let timestamp = Timestamp::now();
        let payload = track_id.into();

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

#[derive(Debug, SendPacket, Clone, Copy, Serialize)]
pub struct IncompatibleVersionPacket {
    pub zero_version: Version,
    pub min_version: Version,
}

impl IncompatibleVersionPacket {
    pub const HEADER_SIZE: usize = size_of::<Self>();

    #[must_use]
    pub fn packet() -> Box<Self> {
        Box::new(Self {
            zero_version: Version::new(0, 0, 0),
            min_version: Version::MIN_COMPATIBLE_VERSION,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[repr(transparent)]
pub struct SecondaryType(u16);

impl SecondaryType {
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }
}

#[cfg(debug_assertions)]
fn valid_secondary_type(st: SecondaryType) -> bool {
    HostControlType::VARIANTS.map(|e| e as u16).contains(&st.0)
        || SessionControlType::VARIANTS
            .map(|e| e as u16)
            .contains(&st.0)
        || PlaybackControlType::VARIANTS
            .map(|e| e as u16)
            .contains(&st.0)
        || ErrorType::VARIANTS.map(|e| e as u16).contains(&st.0)
        || st.0 == 0
}

impl From<ControlType> for SecondaryType {
    fn from(value: ControlType) -> Self {
        let res = match value {
            ControlType::Host(host_control_type) => host_control_type as u16,
            ControlType::Session(session_control_type) => session_control_type as u16,
            ControlType::Playback(playback_control_type) => playback_control_type as u16,
        };
        Self(res)
    }
}

impl From<ErrorType> for SecondaryType {
    fn from(value: ErrorType) -> Self {
        Self(value as u16)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Hash, PartialEq, Eq)]
#[repr(transparent)]
pub struct PacketFingerprint([u8; 16]);

impl<T: Fingerprint> From<&T> for PacketFingerprint {
    fn from(value: &T) -> Self {
        Self(value.fingerprint())
    }
}

#[derive(Deref, DerefMut, Debug, PartialEq, Serialize, Clone)]
pub struct PayloadField(Vec<u8>);

impl PayloadField {
    #[must_use]
    pub fn new(vec: Vec<u8>) -> Self {
        debug_assert!(
            vec.len() <= MAX_PAYLOAD_LENGTH,
            "Invariant broken while constructing `PayloadField`: \
                length of vector larger than `MAX_PAYLOAD_LENGTH` ({} > {})",
            vec.len(),
            MAX_PAYLOAD_LENGTH
        );
        Self(vec)
    }

    #[must_use]
    pub fn empty() -> Self {
        Self(vec![0])
    }

    #[must_use]
    pub fn take(self) -> Vec<u8> {
        self.0
    }
}

impl<T> From<T> for PayloadField
where
    Vec<u8>: From<T>,
{
    fn from(value: T) -> Self {
        Self::new(value.into())
    }
}

impl From<PayloadField> for Box<[u8]> {
    fn from(value: PayloadField) -> Self {
        value.0.into_boxed_slice()
    }
}

#[derive(Clone, Debug, Serialize, Deref, DerefMut)]
pub struct ParityPayload(PayloadField);

impl ParityPayload {
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        let payload = payload.into();
        debug_assert!(
            payload.len() <= ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH,
            "Invariant broken while constructing `PayloadField`: \
                length of vector larger than `ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH` ({} > {})",
            payload.len(),
            ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH
        );
        Self(PayloadField(payload))
    }
}

impl<T> From<T> for ParityPayload
where
    Vec<u8>: From<T>,
{
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl From<ParityPayload> for Box<[u8]> {
    fn from(value: ParityPayload) -> Self {
        value.0.0.into_boxed_slice()
    }
}

#[derive(Debug, Serialize, PartialEq, Default, Clone, Copy)]
#[repr(transparent)]
pub struct BufferId(u16);

impl BufferId {
    #[must_use]
    pub fn new(id: u16) -> Self {
        debug_assert!(
            id != 0,
            "Invariant broken while constructing `BufferId`: \
            a buffer ID can never be 0"
        );

        Self(id)
    }
}

impl Serialize for Option<BufferId> {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.sized() {
            return Err(());
        }
        match self {
            None => {
                buf[0] = 0;
                buf[1] = 0;
            }
            Some(id) => id.serialize(buf)?,
        }
        Ok(())
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        let rep = u16::deserialize(bytes)?;
        match rep {
            0 => Ok(None),
            id => Ok(Some(BufferId::new(id))),
        }
    }

    fn sized(&self) -> usize {
        std::mem::size_of::<BufferId>()
    }
}

#[derive(Deref, Debug, Clone, Copy, Serialize, Display)]
#[repr(transparent)]
pub struct BufferSize(u32);

impl BufferSize {
    const MAX_MB: usize = 10;
    const MAX_BUFFER_SIZE: usize = Self::MAX_MB * 1024 * 1024;

    #[must_use]
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

#[derive(Debug, Serialize, Clone, Copy)]
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

#[derive(
    Eq, Ord, PartialOrd, Deref, DerefMut, PartialEq, Debug, Serialize, Clone, Copy, Display,
)]
#[repr(transparent)]
pub struct BytePosition(pub u32);

impl PartialEq<usize> for BytePosition {
    fn eq(&self, other: &usize) -> bool {
        (**self as usize) == *other
    }
}

impl PartialOrd<usize> for BytePosition {
    fn partial_cmp(&self, other: &usize) -> Option<std::cmp::Ordering> {
        Some((**self as usize).cmp(other))
    }
}

#[allow(clippy::cast_possible_truncation)]
impl From<usize> for BytePosition {
    fn from(value: usize) -> Self {
        BytePosition(value as u32)
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
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

#[derive(Debug, Serialize, Clone, Copy, Eq, PartialOrd, Ord, PartialEq)]
#[repr(transparent)]
pub struct Version(u16);

impl Version {
    pub const CURRENT_VERSION: Version = Version::new(0, 0, 1);
    pub const MIN_COMPATIBLE_VERSION: Version = Version::new(0, 0, 1);

    #[inline]
    #[must_use]
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self((major as u16) << 12 | (minor as u16) << 8 | patch as u16)
    }

    #[inline]
    #[must_use]
    pub const fn parse(&self) -> (u8, u8, u8) {
        (
            (self.0 >> 12) as u8,
            ((self.0 >> 8) & 0xF) as u8,
            (self.0 & 0xFF) as u8,
        )
    }

    #[inline]
    #[must_use]
    pub fn is_compatible(&self) -> bool {
        *self >= Version::MIN_COMPATIBLE_VERSION
    }

    #[inline]
    #[must_use]
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
#[derive(Display, Debug, Serialize, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum PacketType {
    Data = 1,
    Metadata = 2,
    Parity = 3,
    Ack = 4,
    HandshakeAck = 5,
    Host = 7,
    Session = 8,
    Playback = 9,
    Error = 10,
    KeepAlive = 11,
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

#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
#[repr(u8)]
#[variants_array]
pub enum HostControlType {
    Hello = 201,
    HandshakeReject = 202,
}

impl From<HostControlType> for ControlType {
    #[inline]
    fn from(value: HostControlType) -> Self {
        Self::Host(value)
    }
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
#[repr(u8)]
#[variants_array]
pub enum SessionControlType {
    Retransmit = 1,
    TrackRequest = 2,
    MetadataRequest = 3,
    TrackReject = 4,
    Close = 5,
}

impl From<SessionControlType> for ControlType {
    #[inline]
    fn from(value: SessionControlType) -> Self {
        Self::Session(value)
    }
}

#[derive(Debug, Display, Serialize, Clone, Copy, PartialEq)]
#[repr(u8)]
#[variants_array]
pub enum PlaybackControlType {
    Play = 101,
    Pause = 102,
    Close = 103,
    Done = 104,
    Seek = 105,
}

impl From<PlaybackControlType> for ControlType {
    #[inline]
    fn from(value: PlaybackControlType) -> Self {
        Self::Playback(value)
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
#[repr(u8)]
#[variants_array]
pub enum ErrorType {
    UnexpectedPacket = 1,
    IncomprehensiblePacket,
    SessionDoesNotExist,
}

#[derive(Debug, Display, Deref, DerefMut, Serialize, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BatchID(u16);

impl BatchID {
    #[must_use]
    pub fn new(id: u16) -> Self {
        debug_assert!(
            id != 0,
            "Invariant broken while constructing `BatchID`:\
        ID must never be 0."
        );

        Self(id)
    }
}

#[derive(Debug, Flags, Serialize, PartialEq, Clone, Copy)]
#[repr(transparent)]
#[flagtype(OptionFlags)]
pub struct Options(u16);

#[cfg(debug_assertions)]
fn assert_opts_valid(opts: Options, contructing: &'static str) {
    debug_assert!(
        opts.valid_flag(),
        "Invariant broken while constructing `{contructing}`: \
            opts had an invalid value ({})",
        opts.0
    );
}

#[derive(Debug, Clone, Copy, Display)]
#[repr(u16)]
#[variants_array]
pub enum OptionFlags {
    RequireAck = 1 << 0,
    Metadata = 1 << 1,
    Resend = 1 << 2,
}

#[repr(transparent)]
#[derive(Debug, Serialize, Deref, PartialEq, Eq, Hash, Clone, Copy, Display)]
pub struct SessionId(u64);

impl SessionId {
    #[inline]
    #[must_use]
    pub fn generate() -> Self {
        Self(rand::random::<u64>())
    }
}

impl SessionId {
    #[must_use]
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[repr(C)]
#[derive(Debug, Serialize, Clone, Copy, PartialEq)]
pub struct FECInfo {
    pub batch_size: u8,
    pub batch_pos: u8,
    pub recovery_count: u8,
}

impl FECInfo {
    #[must_use]
    pub const fn const_new(batch_size: u8, batch_pos: u8, recovery_count: u8) -> Self {
        Self {
            batch_size,
            batch_pos,
            recovery_count,
        }
    }

    #[must_use]
    pub fn new(batch_size: u8, batch_pos: u8, recovery_count: u8) -> Self {
        debug_assert!(
            batch_pos < batch_size + recovery_count,
            "Invariant broken while constructing `FECInfo`: \
            `batch_pos` is bigger than `batch_size` + `recovery_size` ({batch_pos} >= {batch_size} + {recovery_count})"
        );
        debug_assert!(
            recovery_count <= batch_size,
            "Invariant broken while constructing `FECInfo`: \
            there are more recovery shards than there are data shards ({recovery_count} > {batch_size})"
        );
        Self {
            batch_size,
            batch_pos,
            recovery_count,
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct ByteRange {
    pub start: BytePosition,
    pub length: u16,
}

impl ByteRange {
    #[must_use]
    pub const fn new(start: BytePosition, length: u16) -> Self {
        debug_assert!(
            length as usize <= MAX_PAYLOAD_LENGTH,
            "Invariant broken while constructing a `ByteRange`:\
            `length` is too big. To combine multiple continous ranges, use `Self::concat()`"
        );
        Self { start, length }
    }

    /// On-wire size of a single `ByteRange` (independent of the in-memory
    /// `size_of::<ByteRange>()`, which includes alignment padding).
    #[inline]
    #[must_use]
    pub fn elem_size() -> usize {
        Self::new(BytePosition(0), 0).sized()
    }

    pub fn concat(&mut self, other: &ByteRange) -> bool {
        //debug_assert!(
        //    self.start.0 + self.length as u32 == other.start.0
        //        || other.start.0 + other.length as u32 == self.start.0,
        //    "Invariant broken while trying to concatincate two `ByteRange`s: The two are not continous. \
        //    self: {self:?}, other: {other:?}"
        //);

        if (self.start.0 + self.length as u32 == other.start.0
            || other.start.0 + other.length as u32 == self.start.0)
            && let Some(res) = self.length.checked_add(other.length)
        {
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
        let stride = ByteRange::elem_size();
        if buf.len() < self.len() * stride {
            Err(())
        } else {
            for (i, e) in self.iter().enumerate() {
                e.serialize(&mut buf[i * stride..])?;
            }
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        let stride = ByteRange::elem_size();
        if bytes.len() < stride {
            return Err(());
        }
        // Deserialize fixed-stride chunks. A single pass over `bytes.chunks(stride)`
        // covers every range including the last one — the previous "push, then
        // flush when buf >= stride" loop dropped the trailing chunk.
        bytes
            .chunks(stride)
            .filter(|c| c.len() == stride)
            .map(ByteRange::deserialize)
            .collect()
    }

    fn sized(&self) -> usize {
        self.len() * ByteRange::elem_size()
    }
}
