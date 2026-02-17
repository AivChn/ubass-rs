use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
    usize,
};

use wincode::{SchemaRead, SchemaWrite};

use crate::serialize::*;
use ubass_macros::{self, PacketSerialize};

// =================== DEFINITIONS =================================|

pub const MAX_PAYLOAD_LENGTH: usize = 1400;

#[derive(Debug)]
pub enum PacketWrapper {
    DataPacket(DataPacket),
    AckPacket(AckPacket),
    ControlPacket(ControlPacket),
}

#[derive(Debug, Clone, Copy, Eq, PartialOrd, Ord, PartialEq, SchemaWrite, SchemaRead)]
#[repr(transparent)]
pub struct Version(u16);

/// Enum of all possible packet types as of now
#[derive(PacketSerialize, Clone, Copy, PartialEq, Debug, SchemaWrite, SchemaRead)]
#[repr(u8)]
pub enum PacketType {
    Data = 1,
    Metadata = 2,
    Parity = 3,
    Ack = 4,
    Control = 5,
    ConnectionStat = 6,
}

#[derive(Debug, Clone, Copy)]
pub struct PacketTypeFecBatchID(pub PacketType, pub u16);

#[repr(transparent)]
#[derive(Debug, PartialEq, SchemaRead, SchemaWrite)]
pub struct Options(u16);

#[derive(Clone, Copy)]
pub enum OptionFlags {
    RequireAck = 0b1000,
    SessionEncrypted = 0b1,
}

#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, SchemaWrite, SchemaRead)]
pub struct SessionId(pub u64);

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, SchemaWrite, SchemaRead)]
pub struct FecInfo {
    pub batch_size: u8,
    pub batch_pos: u8,
}

#[repr(C)]
#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct DataPacket {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FecInfo,     // 16
    pub session_id: SessionId, // 64
    // encrypted
    pub timestamp_ms: u64,                 // 64
    pub byte_range_start: u32,             //32
    pub byte_range_offset: u16,            //16
    pub payload_length: u16,               // 16
    pub payload: [u8; MAX_PAYLOAD_LENGTH], // 1400
}

#[repr(C)]
#[derive(Debug, SchemaRead, SchemaWrite)]
pub struct ParityPacket {
    pub version: Version, // 16
    pub opts: Options,    // 16
    pub packet_type_batch_id: PacketTypeFecBatchID,
    pub fec_info: FecInfo,     // 16
    pub session_id: SessionId, // 64
    // encrypted
    pub timestamp_ms: u64,                                     // 64
    pub payload_length: u16,                                   // 16
    pub payload: [u8; ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH], // payload includes data payload and byte range info
}

#[repr(C)]
#[derive(Debug, PartialEq, SchemaWrite, SchemaRead)]
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
    pub session_id: SessionId,
    // encrypted
    pub timestamp_ms: u64,
    pub payload_length: u16,
    pub payload: [u8; MAX_PAYLOAD_LENGTH],
}

// ===================== IMPLEMENTATIONS ===================================|

impl Version {
    pub const CURRENT_VERSION: Version = Version::new(0, 0, 1);
    pub const MAX_COMPATIBLE_VERSION: Version = Version::new(0, 0, 1);
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
        *self <= Version::MAX_COMPATIBLE_VERSION && *self >= Version::MIN_COMPATIBLE_VERSION
    }

    #[inline]
    pub const fn from_bytes(bytes: &[u8; 2]) -> Self {
        Self((bytes[1] as u16) << 8 | bytes[0] as u16)
    }

    #[inline]
    pub const fn to_bytes(&self) -> [u8; 2] {
        [(self.0 >> 8) as u8, (self.0 & 0xFF) as u8]
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (major, minor, patch) = self.parse();
        f.write_str(&format!("{}.{}.{}", major, minor, patch))
    }
}

impl PacketType {
    const VARIANTS: [PacketType; 6] = [
        PacketType::Ack,
        PacketType::Data,
        PacketType::ConnectionStat,
        PacketType::Metadata,
        PacketType::Control,
        PacketType::Parity,
    ];
}

impl TryFrom<u8> for PacketType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if let Some(val) = PacketType::VARIANTS
            .iter()
            .filter(|x| (**x as u8) == value)
            .collect::<Vec<&PacketType>>()
            .get(0)
        {
            Ok(**val)
        } else {
            Err(())
        }
    }
}

impl<'de> SchemaRead<'de> for PacketTypeFecBatchID {
    type Dst = PacketTypeFecBatchID;

    const TYPE_META: wincode::TypeMeta = wincode::TypeMeta::Static {
        size: 2,
        zero_copy: false,
    };

    fn read(
        reader: &mut impl wincode::io::Reader<'de>,
        dst: &mut std::mem::MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        let Ok(bytes) = reader.fill_array::<2>() else {
            return Err(wincode::ReadError::LengthEncodingOverflow(
                "not enough bytes to get type and batch ID",
            ));
        };

        let Ok(packet_type) = PacketType::try_from((bytes[0] & 0xFC) >> 2) else {
            return Err(wincode::ReadError::InvalidCharLead(bytes[0]));
        };

        let batch_id = (((bytes[0] & 3) as u16) << 8) | bytes[1] as u16;

        dst.write(PacketTypeFecBatchID(packet_type, batch_id));

        Ok(())
    }
}

impl SchemaWrite for PacketTypeFecBatchID {
    type Src = PacketTypeFecBatchID;

    const TYPE_META: wincode::TypeMeta = wincode::TypeMeta::Static {
        size: 2,
        zero_copy: false,
    };

    fn size_of(src: &Self::Src) -> wincode::WriteResult<usize> {
        _ = src;
        Ok(2)
    }

    fn write(writer: &mut impl wincode::io::Writer, src: &Self::Src) -> wincode::WriteResult<()> {
        let serialized = ((src.0 as u16) << 10) | (src.1 & 1023); // 1023 == 10 set bits
        writer
            .write(&[(serialized >> 8) as u8, (serialized & 0xFF) as u8][..])
            .map_err(|err| wincode::WriteError::Io(err))
    }
}
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

    pub fn from_bytes(bytes: &[u8; 8]) -> Self {
        let temp: u64 =
            wincode::deserialize(&bytes[..]).expect("8 bytes should be equal to 64 bits");
        Self(temp)
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

impl DataPacket {
    pub const HEADER_SIZE: usize = size_of::<DataPacket>() - MAX_PAYLOAD_LENGTH;
    pub const MIN_SIZE: usize = DataPacket::HEADER_SIZE + 1;
}

impl ParityPacket {
    pub const LOCAL_MAX_PAYLOAD_LENGTH: usize = MAX_PAYLOAD_LENGTH + 8;
    pub const HEADER_SIZE: usize =
        size_of::<ParityPacket>() - ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH;
    pub const MIN_SIZE: usize = ParityPacket::HEADER_SIZE + 9;

    pub fn new(
        payload: [u8; Self::LOCAL_MAX_PAYLOAD_LENGTH],
        opts: Options,
        packet_type_batch_id: PacketTypeFecBatchID,
        fec_info: FecInfo,
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
            payload,
        }
    }
}

impl AckPacket {
    pub const HEADER_SIZE: usize = size_of::<AckPacket>();
    pub const MIN_SIZE: usize = AckPacket::HEADER_SIZE;
}

impl ControlPacket {
    pub const HEADER_SIZE: usize = size_of::<ControlPacket>() - MAX_PAYLOAD_LENGTH;
    pub const MIN_SIZE: usize = ControlPacket::HEADER_SIZE + 1;
}
