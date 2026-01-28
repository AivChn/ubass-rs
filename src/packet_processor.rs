use crate::packetizer::{ControlPacket, DataPacket, PacketType, PacketWrapper};

/// Enum used to send messages to the transport send task
/// currently can either be data or an instruction to close the task gracefully
/// Upon recieving Close, the task will wait to confirm all packets were sent
#[derive(Debug, Clone)]
pub enum TransportSendMessage {
    Data(Vec<ProcessedPacket>),
    Close,
}

// a struct used to identify a packet uniquely, used for resending when necessary mostly.
// timestamp is taken from the headers of the packet as they are produced from the packetizer
// layer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PacketId {
    pub timestamp: u64,
    pub session_id: u64,
}

/// a struct that represents the serialized packet with the minimal data necessary for the
/// transport layer to process it correctly.
#[derive(Clone, Debug)]
pub struct ProcessedPacket {
    pub packet_id: PacketId,
    pub packet_type: PacketType,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}

pub fn process_packet(packet: PacketWrapper) -> ProcessedPacket {
    match packet {
        PacketWrapper::DataPacket(pack) => ProcessedPacket {
            packet_id: PacketId {
                timestamp: pack.timestamp_ms,
                session_id: pack.session_id,
            },
            packet_type: pack.packet_type,
            data: wincode::serialize(&pack).expect("I didnt handle this yet")
                [..pack.payload_length as usize + DataPacket::HEADER_SIZE]
                .to_vec(),
            duplicate_count: 1,
        },
        PacketWrapper::AckPacket(pack) => ProcessedPacket {
            packet_id: PacketId {
                timestamp: pack.timestamp_ms,
                session_id: pack.session_id,
            },
            packet_type: pack.packet_type,
            data: wincode::serialize(&pack).expect("I didnt handlet this yet"),
            duplicate_count: 5,
        },
        PacketWrapper::ControlPacket(pack) => ProcessedPacket {
            packet_id: PacketId {
                timestamp: pack.timestamp_ms,
                session_id: pack.session_id,
            },
            packet_type: pack.packet_type,
            data: wincode::serialize(&pack).expect("I didnt handle this yet")
                [..pack.payload_length as usize + ControlPacket::HEADER_SIZE]
                .to_vec(),
            duplicate_count: 3,
        },
    }
}
