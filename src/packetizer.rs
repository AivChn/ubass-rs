use std::fmt::Display;

use wincode::{SchemaRead, SchemaWrite};

const MAX_PAYLOAD_LENGTH: u16 = 1400;

#[repr(C)]
#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct PacketHeaders {
    version: Version,
    opts: Options,
    packet_type: PacketType,
    reserved: u8,
    fec_info: FecInfo,
    session_id: u64,
}

pub trait Packet {
    pub fn get_headers(&self) -> PacketHeaders;
}

#[derive(Debug, Clone, Copy, PartialEq, SchemaWrite, SchemaRead)]
#[repr(transparent)]
pub struct Version(u16);

pub impl Version {
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
#[derive(Clone, Copy, PartialEq, Debug, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u16")]
pub enum Options {
    #[wincode(tag = 0b1000)]
    RequireAck,
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

impl Packet for DataPacket {
    fn get_headers(&self) -> PacketHeaders {
        PacketHeaders {
            version: self.version,
            opts: self.opts,
            packet_type: self.packet_type,
            reserved: 0,
            fec_info: self.fec_info,
            session_id: self.session_id,
        }
    }
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
pub struct AckPacket {
    pub version: Version,
    pub opts: Options,
    pub packet_type: PacketType,
    pub reserved: u8,
    pub session_id: u64,
    // encrypted
    pub timestamp_ms: u64,
    pub ack_timestamp_ms: u64,
    pub ack_opts: Options,
    pub ack_packet_type: PacketType,
}

impl Packet for AckPacket {
    fn get_headers(&self) -> PacketHeaders {
        PacketHeaders {
            version: self.version,
            opts: self.opts,
            packet_type: self.packet_type,
            reserved: 0,
            session_id: self.session_id,
        }
    }
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

impl Packet for ControlPacket {}
