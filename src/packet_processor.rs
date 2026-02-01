use std::usize;

use futures::channel::mpsc::Sender;
use tokio::sync::mpsc::Receiver;

use crate::{
    InternalError,
    packetizer::{
        AckPacket, ControlPacket, DataPacket, MAX_PAYLOAD_LENGTH, Options, PacketType,
        PacketTypeFecBatchID, PacketWrapper, SessionId, Version,
    },
    transport::{ReceivedPacket, TransportError},
};

pub enum PacketProcessingMessage {
    SendPacket(PacketWrapper),
    Close,
}

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
pub enum PacketProcessingError {
    Internal(InternalError),
    PacketTypeNotIMplemented(PacketType),
    IncompatibleVersion(Version),
    WrongHeaderSize(usize),
    InvalidPacketTypeHeader(u8),
    FailedToDeserialize,
}

pub async fn init(
    p_receiver: Receiver<PacketProcessingMessage>,
    p_sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
    t_receiver: Receiver<Result<ReceivedPacket, TransportError>>,
    t_sender: Sender<TransportSendMessage>,
) -> Result<(), PacketProcessingError> {
    let mut recv_handle = tokio::spawn(recv(t_receiver, p_sender.clone()));
    let mut send_handle = tokio::spawn(send(t_sender.clone(), p_sender.clone(), p_receiver));

    'supervisor: loop {
        _ = tokio::select! {
            res = &mut recv_handle, if !recv_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(PacketProcessingError::Internal(InternalError::TaskFailed));
                };

                match result {
                    // TODO: update error handling
                    Err(e) => Err::<(), _>(e),
                    Ok(()) => break 'supervisor Ok(()),
                }
            },
            res = &mut send_handle, if !send_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(PacketProcessingError::Internal(InternalError::TaskFailed));
                };

                match result {
                    // TODO: update error handling
                    Err(e) => Err(e),
                    Ok(()) => { recv_handle.abort(); break 'supervisor Ok(())},
                }
            }
        }
    }
}

async fn recv(
    mut t_receiver: Receiver<Result<ReceivedPacket, TransportError>>,
    p_sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
) -> Result<(), PacketProcessingError> {
    // wait on receive
    loop {
        let Some(packet) = t_receiver.recv().await else {
            return Err(PacketProcessingError::Internal(
                InternalError::ChannelClosed,
            ));
        };

        _ = packet;
    }

    // decrypt

    // == if data or parity ==

    // save to FEC table

    // === if batch ended ===

    // discard batch

    // === else ===

    // ==== if packet wait time exceeded ====

    // ===== if there is enough packets to derive =====

    // use FEC

    // ===== else =====

    // send to packetizer missing ranges

    // == else ==

    // save identifying info

    // == end ==

    // deserialize

    // send to packetizer
}

async fn send(
    t_sender: Sender<TransportSendMessage>,
    p_sender: Sender<Result<ProcessedPacket, PacketProcessingError>>,
    p_receiver: Receiver<PacketProcessingMessage>,
) -> Result<(), PacketProcessingError> {
    // TODO: implement send pipeline
    todo!("implement send pipeline")

    // Wait on receive

    //serialize

    // save copy for parity derivition

    // encrypt

    // send to transport

    // if final packet in batch
    //  derive parity
    //  send parity
    //  calculate new batch size

    // repeat
}

async fn process_received_packet(
    received_packet: ReceivedPacket,
    sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
) -> Result<(), PacketProcessingError> {
    let processed = match process_serialized(received_packet) {
        Ok(packet) => packet,
        Err(err) => todo!("error handling"),
    };

    let key = get_key_from_session(processed.packet_id.session_id)
        .expect("literal value returned as Some");
    let decrypted_data = decrypt(processed.data, key);

    let packet = match deserialize(ProcessedPacket {
        packet_id: processed.packet_id,
        packet_type: processed.packet_type,
        data: decrypted_data,
        duplicate_count: processed.duplicate_count,
    }) {
        Ok(packet) => packet,
        Err(err) => todo!("error handling"),
    };

    Ok(())
}

fn get_key_from_session(session_id: SessionId) -> Option<u128> {
    Some(34215909873652376164537433124 as u128)
}

fn decrypt(packet: Vec<u8>, key: u128) -> Vec<u8> {
    _ = key;
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

    let Ok(packet_type) = PacketType::try_from(packet.data[4]) else {
        return Err(PacketProcessingError::InvalidPacketTypeHeader(
            packet.data[4],
        ));
    };

    match packet_type {
        PacketType::Data | PacketType::Parity => {
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

fn deserialize(mut packet: ProcessedPacket) -> Result<PacketWrapper, PacketProcessingError> {
    match packet.packet_type {
        PacketType::Data => {
            packet
                .data
                .resize(DataPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            let Ok(deserialized) = wincode::deserialize::<DataPacket>(&packet.data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::DataPacket(deserialized))
        }
        PacketType::Control => {
            packet
                .data
                .resize(ControlPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            let Ok(deserialized) = wincode::deserialize::<ControlPacket>(&packet.data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::ControlPacket(deserialized))
        }
        PacketType::Ack => {
            packet.data.resize(AckPacket::HEADER_SIZE, 0);
            let Ok(deserialized) = wincode::deserialize::<AckPacket>(&packet.data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::AckPacket(deserialized))
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
            packet_type: pack.packet_type_batch_id.0,
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
