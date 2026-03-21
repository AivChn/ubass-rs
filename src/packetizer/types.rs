#[allow(unused_imports)]
use crate::prelude::*;

use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::packet_processor::serialize::{PacketDeserialize, PacketSerialize};
use aes_gcm_siv::aead::Payload;
use derive_more::Display;
use reed_solomon_simd::Recovery;
use tokio::time::Instant;
use ubass_macros::{PacketDeserialize, PacketSerialize};

pub const MAX_PAYLOAD_LENGTH: usize = 1400;

#[derive(Debug)]
pub enum PacketWrapper {
    DataPacket(DataPacket),
    AckPacket(AckPacket),
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
    Control = 5,
    ConnectionStat = 6,
    Host = 7,
    Session = 8,
}

pub type BatchID = u16;

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
    pub fn contains(&self, flag: OptionFlags) -> bool {
        self.0 & (flag as u16) != 0
    }

    #[must_use]
    pub fn remove(mut self, flags: &[OptionFlags]) -> Self {
        self.0 &= !flags
            .iter()
            .map(|x| *x as u16)
            .reduce(|acc, x| acc | x)
            .unwrap_or(0);
        self
    }

    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub fn add(mut self, flags: &[OptionFlags]) -> Self {
        self.0 |= flags
            .iter()
            .map(|x| *x as u16)
            .reduce(|acc, x| acc | x)
            .unwrap_or(0);
        self
    }

    // TODO: make a macro to get the list of all variants from an enum
    pub fn deconstruct(&self) -> Vec<OptionFlags> {
        let mut opts = vec![];
        if (OptionFlags::RequireAck as u16) & self.0 != 0 {
            opts.push(OptionFlags::RequireAck);
        }
        opts
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum OptionFlags {
    RequireAck = 1 << 0,
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
    pub recovery_size: u8,
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
            recovery_size,
        }
    }
}

#[repr(C)]
#[derive(Debug, PacketDeserialize, PacketSerialize)]
struct ByteRange {
    start: u32,
    length: u16,
}

impl ByteRange {
    pub fn new(start: u32, length: u16) -> Self {
        debug_assert!(
            length as usize <= MAX_PAYLOAD_LENGTH,
            "Invariant broken while constructing a `ByteRange`:\
            `length` is too big ({length}). To combine multiple continous ranges, use `Self::concat()`"
        );
        Self { start, length }
    }

    pub fn concat(&mut self, other: &ByteRange) -> bool {
        debug_assert!(
            self.start + self.length as u32 == other.start
                || other.start + other.length as u32 == self.start,
            "Invariant broken while trying to concatincate two `ByteRange`s: The two are not continous. \
            self: {self:?}, other: {other:?}"
        );

        if u16::try_from(self.length as u32 + other.length as u32).is_ok() {
            self.length += other.length;
            if other.start + other.length as u32 == self.start {
                self.start = other.start;
            }
            true
        } else {
            false
        }
    }
}

// TODO: finalize after encryption is understood
#[repr(C)]
#[derive(PacketSerialize, PacketDeserialize)]
struct HelloPacket {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: u16,
    pub timestamp_ms: u64,
    pub proposed_session_id: u64,
    pub encryption_public_key: Key,
    pub signing_public_key: Key,
}

#[repr(C)]
#[derive(PacketSerialize, PacketDeserialize)]
struct RetransmitPacket {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: u16,
    pub session_id: SessionId,
    pub payload: Vec<ByteRange>,
}

impl RetransmitPacket {
    // closest I can get to `MAX_PAYLOAD_LENGTH` while aligning to 6 bytes
    const LOCAL_MAX_PAYLOAD_LENGTH: usize = 1398;
    const HEADER_SIZE: usize = size_of::<Self>() - Self::LOCAL_MAX_PAYLOAD_LENGTH;

    pub fn new(opts: Options, session_id: SessionId, payload: Vec<ByteRange>) -> Self {
        debug_assert!(
            payload.len() <= (Self::LOCAL_MAX_PAYLOAD_LENGTH / size_of::<ByteRange>()),
            "Invariant broken while constructing a `RetransmitPacket`: payload bigger than allowed max size: {} `ByteRange`s ({} bytes) > {} `ByteRange`s ({} bytes)",
            payload.len(),
            (payload.len() * size_of::<ByteRange>()),
            (Self::LOCAL_MAX_PAYLOAD_LENGTH / size_of::<ByteRange>()),
            Self::LOCAL_MAX_PAYLOAD_LENGTH
        );
        Self {
            version: Version::CURRENT_VERSION,
            opts: opts.add(&[OptionFlags::RequireAck]),
            packet_type: PacketType::Session,
            control_type: ControlType::Retransmit,
            reserved: 0,
            session_id,
            payload,
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

#[repr(C)]
#[derive(PacketSerialize)]
struct TrackRequestPacket {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: u16,
    pub session_id: SessionId,
    pub payload: Box<[u8; MAX_PAYLOAD_LENGTH]>,
}

impl TrackRequestPacket {
    pub fn new(
        opts: Options,
        session_id: SessionId,
        payload: Box<[u8; MAX_PAYLOAD_LENGTH]>,
    ) -> Self {
        Self {
            version: Version::CURRENT_VERSION,
            opts: opts.add(&[OptionFlags::RequireAck]),
            packet_type: PacketType::Session,
            control_type: ControlType::TrackRequest,
            reserved: 0,
            session_id,
            payload,
        }
    }
}

// TODO: figure this shit out with the hello packet
#[repr(C)]
struct SessionKeyPart {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type: PacketType,
    pub control_type: ControlType,
    pub reserved: u16,
    pub session_id: SessionId,
    pub encryption_n_value: Key,
    pub encryption_g_value: u128,
    pub encryption_key_part: Key,
    pub mac_n_value: Key,
    pub mac_g_value: u128,
    pub mac_key_part: Key,
}

#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Debug)]
pub struct DataPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp_ms: u64,
    pub byte_range_start: u32,
    pub payload: Vec<u8>,
}

impl DataPacket {
    pub const HEADER_SIZE: usize =
        size_of::<DataPacket>() - size_of::<Box<[u8; MAX_PAYLOAD_LENGTH]>>();
    pub const MIN_SIZE: usize = DataPacket::HEADER_SIZE + 1;

    pub fn new(
        opts: Options,
        batch_id: BatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        byte_range_start: u32,
        payload: Vec<u8>,
    ) -> Self {
        let version = Version::CURRENT_VERSION;
        let packet_type_batch_id = PacketTypeFecBatchID {
            packet_type: PacketType::Data,
            batch_id,
        };
        let timestamp_ms = Instant::now()
            .duration_since(*PROTOCOL_EPOCH.get_or_init(Instant::now))
            .as_millis() as u64;

        Self {
            version,
            opts,
            packet_type_batch_id,
            fec_info,
            session_id,
            timestamp_ms,
            byte_range_start,
            payload,
        }
    }
}

#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Debug)]
pub struct ParityPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FECInfo,
    pub session_id: SessionId,
    pub timestamp_ms: u64,
    pub payload: Vec<u8>,
}

impl ParityPacket {
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH + 4;
    pub const HEADER_SIZE: usize = size_of::<ParityPacket>() - size_of::<Vec<u8>>();
    pub const MIN_SIZE: usize = ParityPacket::HEADER_SIZE + 9;

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
        let timestamp_ms = Instant::now()
            .duration_since(*PROTOCOL_EPOCH.get_or_init(Instant::now))
            .as_millis() as u64;

        Self {
            version,
            opts,
            packet_type_batch_id,
            fec_info,
            session_id,
            timestamp_ms,
            payload,
        }
    }
}
#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Debug, PartialEq)]
pub struct AckPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    reserved: u8,
    pub session_id: SessionId,
    // encrypted
    pub timestamp_ms: u64,
    pub ack_timestamp_ms: u64,
    pub ack_opts: Options,
    pub ack_packet_type: PacketType,
}

impl AckPacket {
    pub const HEADER_SIZE: usize = size_of::<AckPacket>();
    pub const MIN_SIZE: usize = AckPacket::HEADER_SIZE;

    pub fn new(
        opts: Options,
        session_id: SessionId,
        ack_timestamp_ms: u64,
        ack_opts: Options,
        ack_packet_type: PacketType,
    ) -> Self {
        debug_assert!(
            !opts.contains(OptionFlags::RequireAck),
            "Invariant broken while constructing `AckPacket`: \
            flag `RequireAck` was present, which should not be allowed."
        );
        #[allow(clippy::cast_possible_truncation)]
        Self {
            version: Version::CURRENT_VERSION,
            opts,
            packet_type: PacketType::Ack,
            reserved: 0,
            session_id,
            timestamp_ms: Instant::now()
                .duration_since(*PROTOCOL_EPOCH.get_or_init(Instant::now))
                .as_millis() as u64,
            ack_timestamp_ms,
            ack_opts,
            ack_packet_type,
        }
    }
}

#[derive(PacketDeserialize, PacketSerialize, Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum ControlType {
    Hello,
    Retransmit,
    Play,
    Stop,
    Restart,
    Pause,
    Seek,
    TrackRequest,
    SessionKeyOffer,
}
