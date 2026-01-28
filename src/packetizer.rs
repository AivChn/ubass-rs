use std::{fmt::Display, usize};

use wincode::{SchemaRead, SchemaWrite};

// =================== DEFINITIONS =================================|

const MAX_PAYLOAD_LENGTH: usize = 1400;

#[derive(Debug)]
pub enum PacketWrapper {
    DataPacket(DataPacket),
    AckPacket(AckPacket),
    ControlPacket(ControlPacket),
}

#[derive(Debug, Clone, Copy, PartialEq, SchemaWrite, SchemaRead)]
#[repr(transparent)]
pub struct Version(u16);

/// Enum of all possible packet types as of now
#[derive(Clone, Copy, PartialEq, Debug, SchemaWrite, SchemaRead)]
#[wincode(tag_encoding = "u8")]
pub enum PacketType {
    Data,
    Metadata,
    Parity,
    Ack,
    Control,
    ConnectionStat,
}

#[repr(transparent)]
#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct Options(u16);

#[derive(Clone, Copy)]
enum OptionFlags {
    RequireAck = 0b1000,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, SchemaWrite, SchemaRead)]
pub struct FecInfo {
    batch_size: u8,
    batch_pos: u8,
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct DataPacket {
    pub version: Version,        // 16
    pub opts: Options,           // 16
    pub packet_type: PacketType, // 8
    reserved: u8,                // 8
    pub fec_info: FecInfo,       // 16
    pub session_id: u64,         // 64
    // encrypted
    pub timestamp_ms: u64,                 // 64
    pub byte_range_start: u32,             //32
    pub byte_range_offset: u16,            //16
    pub payload_length: u16,               // 16
    pub payload: [u8; MAX_PAYLOAD_LENGTH], // 1400
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
pub struct AckPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    reserved: u8,
    pub session_id: u64,
    // encrypted
    pub timestamp_ms: u64,
    pub ack_timestamp_ms: u64,
    pub ack_opts: Options,
    pub ack_packet_type: PacketType,
}

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum ControlType {
    Hello,
    Retransmit,
    Play,
    Stop,
    Restart,
    Pause,
    Seek,
    SendMetadata,
    NewEncryptionKey,
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
pub struct ControlPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub control_type: ControlType,
    reserved: u16,
    pub session_id: u64,
    // encrypted
    pub timestamp_ms: u64,
    pub payload_length: u16,
    pub payload: [u8; MAX_PAYLOAD_LENGTH],
}

// ===================== IMPLEMENTATIONS ===================================|

impl Version {
    pub const CURRENT_VERSION: Version = Version::new(0, 0, 1);
    pub const MAX_ALLOWED_VERSION: Version = Version::new(0, 0, 1);
    pub const MIN_ALLOWED_VERSION: Version = Version::new(0, 0, 1);
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self((major as u16) << 12 | (minor as u16) << 8 | patch as u16)
    }
    pub const fn parse(&self) -> (u8, u8, u8) {
        (
            (self.0 >> 12) as u8,
            ((self.0 >> 8) & 0xF) as u8,
            (self.0 & 0xFF) as u8,
        )
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (major, minor, patch) = self.parse();
        f.write_str(&format!("{}.{}.{}", major, minor, patch))
    }
}

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
}

impl DataPacket {
    pub const HEADER_SIZE: usize = size_of::<DataPacket>() - MAX_PAYLOAD_LENGTH;
}

impl AckPacket {
    pub const HEADER_SIZE: usize = size_of::<AckPacket>();
}

impl ControlPacket {
    pub const HEADER_SIZE: usize = size_of::<ControlPacket>() - MAX_PAYLOAD_LENGTH;
}
