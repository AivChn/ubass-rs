use std::usize;

use crate::{
    packetizer::{
        AckPacket, ControlPacket, DataPacket, MAX_PAYLOAD_LENGTH, Options, PacketType,
        PacketWrapper, SessionId, Version,
    },
    transport::ReceivedPacket,
};

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
    pub session_id: SessionId,
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

#[derive(Debug)]
enum PacketProcessingError {
    PacketTypeNotIMplemented(PacketType),
    IncompatibleVersion(Version),
    WrongHeaderSize(usize),
    InvalidPacketTypeHeader(u8),
    FailedToDeserialize,
}

fn decrypt(packet: Vec<u8>) -> Vec<u8> {
    packet
}

fn serialize_packet(wrapped_packet: PacketWrapper) -> Option<Vec<u8>> {
    match wrapped_packet {
        PacketWrapper::DataPacket(packet) => {
            if let Ok(serialized) = wincode::serialize(&packet) {
                Some(serialized[..packet.payload_length as usize].to_vec())
            } else {
                None
            }
        }
        PacketWrapper::ControlPacket(packet) => {
            if let Ok(serialized) = wincode::serialize(&packet) {
                Some(serialized[..packet.payload_length as usize].to_vec())
            } else {
                None
            }
        }
        PacketWrapper::AckPacket(packet) => wincode::serialize(&packet).ok(),
    }
}

fn process_serialized(packet: ReceivedPacket) -> Result<ProcessedPacket, PacketProcessingError> {
    if packet.data.len() < 5 {
        return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
    }

    let packet_version = Version::from_bytes(packet.data[1], packet.data[0]);
    if !packet_version.is_compatible() {
        return Err(PacketProcessingError::IncompatibleVersion(packet_version));
    }

    let _opts = Options::from_bytes(packet.data[3], packet.data[2]);

    let Some(packet_type) = PacketType::from_bytes(packet.data[4]) else {
        return Err(PacketProcessingError::InvalidPacketTypeHeader(
            packet.data[4],
        ));
    };

    match packet_type {
        PacketType::Data => {
            if packet.data.len() < DataPacket::MIN_SIZE {
                return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
            }
        }
        PacketType::Control => {
            if packet.data.len() < ControlPacket::MIN_SIZE {
                return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
            }
        }
        PacketType::Ack => {
            if packet.data.len() < AckPacket::MIN_SIZE {
                return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
            }
        }
        // TODO: implement the rest after adding the packets
        _ => return Err(PacketProcessingError::PacketTypeNotIMplemented(packet_type)),
    };

    let _reserved = packet.data[5];

    let session_id = SessionId::from_bytes(
        packet.data[6..14]
            .try_into()
            .expect("an 8 byte slice is the same as an 8 byte array"),
    );

    Ok(ProcessedPacket {
        packet_id: PacketId {
            timestamp: 0,
            session_id: session_id,
        },
        packet_type,
        data: packet.data,
        duplicate_count: 0,
    })
}

fn deserialize(packet: ProcessedPacket) -> Result<PacketWrapper, PacketProcessingError> {
    let mut decrypted_data = decrypt(packet.data);

    match packet.packet_type {
        PacketType::Data => {
            decrypted_data.resize(DataPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            let Ok(temp) = wincode::deserialize::<DataPacket>(&decrypted_data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::DataPacket(temp))
        }
        PacketType::Control => {
            decrypted_data.resize(ControlPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            let Ok(temp) = wincode::deserialize::<ControlPacket>(&decrypted_data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::ControlPacket(temp))
        }
        PacketType::Ack => {
            decrypted_data.resize(AckPacket::HEADER_SIZE, 0);
            let Ok(temp) = wincode::deserialize::<AckPacket>(&decrypted_data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::AckPacket(temp))
        }

        // TODO: implement the rest after adding the packets
        _ => panic!("Havent taken care of this yet"),
    }
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
