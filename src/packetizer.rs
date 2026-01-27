use std::fmt::Display;

use wincode::{SchemaRead, SchemaWrite};

const MAX_PAYLOAD_LENGTH: u16 = 1400;

#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
#[repr(transparent)]
struct Version(u16);

impl Version {
    const CURRENT_VERSION: Version = Version::new(0, 0, 1);
    const MAX_ALLOWED_VERSION: Version = Version::new(0, 0, 1);
    const MIN_ALLOWED_VERSION: Version = Version::new(0, 0, 1);
    const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self((major as u16) << 12 | (minor as u16) << 8 | patch as u16)
    }
    const fn parse(&self) -> (u8, u8, u8) {
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

/// Enum of all possible packet types as of now
#[derive(Clone, PartialEq, Debug, SchemaWrite, SchemaRead)]
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
#[derive(PartialEq, Debug, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u16")]
pub enum Options {
    #[wincode(tag = 0b1000)]
    RequireAck,
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
pub struct FecInfo {
    batch_size: u8,
    batch_pos: u8,
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct DataPacket {
    version: Version,
    opts: Options,
    packet_type: PacketType,
    reserved: u8,
    fec_info: FecInfo,
    session_id: u64,
    // encrypted
    timestamp_ms: u64,
    byte_range_start: u32,
    byte_range_offset: u16,
    payload_length: u16,
    payload: [u8; MAX_PAYLOAD_LENGTH as usize],
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
struct AckPacket {
    version: Version,
    opts: Options,
    packet_type: PacketType,
    reserved: u8,
    session_id: u64,
    // encrypted
    timestamp_ms: u64,
    ack_timestamp_ms: u64,
    ack_opts: Options,
    ack_packet_type: PacketType,
}

#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
enum ControlType {
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
struct ControlPacket {
    version: Version,
    opts: Options,
    packet_type: PacketType,
    control_type: ControlType,
    session_id: u64,
    // encrypted
    timestamp_ms: u64,
    payload_length: u16,
    payload: [u8; MAX_PAYLOAD_LENGTH as usize],
}
