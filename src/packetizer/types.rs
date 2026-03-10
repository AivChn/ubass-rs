#[allow(unused_imports)]
use crate::prelude::*;

use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::packet_processor::serialize::{PacketDeserialize, PacketSerialize};
use ubass_macros::{PacketDeserialize, PacketSerialize};

pub const MAX_PAYLOAD_LENGTH: usize = 1400;

#[derive(Debug)]
pub enum PacketWrapper {
    DataPacket(DataPacket),
    AckPacket(AckPacket),
    ControlPacket(ControlPacket),
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

    #[inline]
    #[deprecated]
    pub const fn from_bytes(bytes: &[u8; 2]) -> Self {
        Self((bytes[1] as u16) << 8 | bytes[0] as u16)
    }

    #[inline]
    #[deprecated]
    pub const fn to_bytes(&self) -> [u8; 2] {
        [(self.0 >> 8) as u8, (self.0 & 0xFF) as u8]
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (major, minor, patch) = self.parse();
        write!(f, "{}.{}.{}", major, minor, patch)
    }
}

/// Enum of all possible packet types as of now
#[derive(PacketDeserialize, PacketSerialize, Clone, Copy, PartialEq, Debug)]
#[repr(u16)]
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
pub struct PacketTypeFecBatchID(pub PacketType, pub BatchID);

#[derive(PacketDeserialize, PacketSerialize, Debug, PartialEq)]
#[repr(transparent)]
pub struct Options(u16);

impl Options {
    pub fn construct(flags: Vec<OptionFlags>) -> Self {
        Self(
            flags
                .iter()
                .map(|x| *x as u16)
                .reduce(|f1, f2| f1 | f2)
                .unwrap_or(0),
        )
    }

    pub fn deconstruct(&self) -> Vec<OptionFlags> {
        let mut opts = vec![];
        if (OptionFlags::RequireAck as u16) & self.0 != 0 {
            opts.push(OptionFlags::RequireAck);
        }

        if (OptionFlags::SessionEncrypted as u16) & self.0 != 0 {
            opts.push(OptionFlags::SessionEncrypted);
        }

        opts
    }

    #[inline]
    pub const fn from_bytes(msb: u8, lsb: u8) -> Self {
        Self((msb as u16) << 8 | (lsb as u16))
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum OptionFlags {
    SessionEncrypted = 1 << 0,
    RequireAck = 1 << 1,
}

#[repr(C)]
#[derive(PacketSerialize, PacketDeserialize)]
struct Key(u128, u128);

#[repr(transparent)]
#[derive(PacketDeserialize, PacketSerialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct SessionId(pub u64);

impl SessionId {
    pub fn new() -> Self {
        Self(
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time should move forward")
                .as_millis()
                * 32413245
                % 324987645983276514357846239847) as u64,
        )
    }
}

#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Clone, Copy, Debug, PartialEq)]
pub struct FECInfo {
    pub batch_size: u8,
    pub batch_pos: u8,
    pub recovery_size: u8,
}

#[repr(C)]
#[derive(Debug, PacketDeserialize, PacketSerialize)]
struct ByteRange {
    pub start: u32,
    pub length: u16,
}

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
    pub payload_length: u16,
    pub payload: Vec<ByteRange>,
}

impl PacketSerialize for Vec<ByteRange> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.len() * size_of::<ByteRange>() {
            false
        } else {
            self.iter()
                .enumerate()
                .map(|(i, e)| e.serialize(&mut buf[i * size_of::<ByteRange>()..]))
                .all(|e| e)
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
    pub payload_length: u16,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive()]
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
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FECInfo,     // 16
    pub session_id: SessionId, // 64
    // encrypted
    pub timestamp_ms: u64, // 64
    pub byte_range_start: u32,
    pub payload: Box<[u8; MAX_PAYLOAD_LENGTH]>, // 1400
}

impl DataPacket {
    pub const HEADER_SIZE: usize =
        size_of::<DataPacket>() - size_of::<Box<[u8; MAX_PAYLOAD_LENGTH]>>();
    pub const MIN_SIZE: usize = DataPacket::HEADER_SIZE + 1;
}

#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Debug)]
pub struct ParityPacket {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FECInfo,     // 16
    pub session_id: SessionId, // 64
    // encrypted
    pub timestamp_ms: u64,                                          // 64
    pub payload_length: u16,                                        // 16
    pub payload: Box<[u8; ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH]>, // payload includes data payload and byte range inf o
}

impl ParityPacket {
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH + 4;
    pub const HEADER_SIZE: usize =
        size_of::<ParityPacket>() - size_of::<Box<[u8; ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH]>>();
    pub const MIN_SIZE: usize = ParityPacket::HEADER_SIZE + 9;

    pub fn new(
        payload: Box<[u8; Self::LOCAL_MAX_PAYLOAD_LENGTH]>,
        opts: Options,
        packet_type_batch_id: PacketTypeFecBatchID,
        fec_info: FECInfo,
        session_id: SessionId,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            version: Version::CURRENT_VERSION,
            opts,
            packet_type_batch_id,
            fec_info,
            session_id,
            timestamp_ms,
            payload_length: Self::LOCAL_MAX_PAYLOAD_LENGTH as u16,
            payload: payload,
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

#[repr(C)]
#[derive(PacketDeserialize, PacketSerialize, Debug, PartialEq)]
pub struct ControlPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    reserved: u16,
    pub session_id: SessionId,
    // encrypted
    pub timestamp_ms: u64,
    pub payload_length: u16,
    pub payload: Box<[u8; MAX_PAYLOAD_LENGTH]>,
}

impl ControlPacket {
    pub const HEADER_SIZE: usize =
        size_of::<ControlPacket>() - size_of::<Box<[u8; MAX_PAYLOAD_LENGTH]>>();
    pub const MIN_SIZE: usize = ControlPacket::HEADER_SIZE + 1;
}

// ===================== IMPLEMENTATIONS ===================================|
