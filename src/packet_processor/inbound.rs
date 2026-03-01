use crate::{
    InternalError,
    packet_processor::serialize::PacketDeserialize,
    packetizer::types::{
        AckPacket, ControlPacket, DataPacket, MAX_PAYLOAD_LENGTH, PacketType, PacketWrapper,
        SessionId, Version,
    },
    transport::types::ReceivedPacket,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::mpsc::Sender;

use super::types::*;

/// Receive pipeline: handles incoming packets from transport layer.
///
/// Processing steps (planned):
/// 1. Receive encrypted packet from transport
/// 2. Decrypt packet data
/// 3. For data/parity packets:
///    - Save to FEC table
///    - If batch complete: discard batch
///    - If wait time exceeded:
///      - If enough packets to derive: use FEC
///      - Otherwise: request retransmission of missing ranges
/// 4. Deserialize packet
/// 5. Forward to packetizer layer
pub async fn init(
    InboundChannels {
        mut t_receiver,
        p_sender,
    }: InboundChannels,
    fec_table: Arc<HashMap<Batch, HashSet<FecPacket>>>,
) -> Result<(), PacketProcessingError> {
    // wait on receive
    loop {
        let packet = match t_receiver.recv().await {
            Some(Ok(packet)) => packet,
            Some(Err(_)) => todo!("ERROR HANDLING!!!!!"),
            None => {
                return Err(PacketProcessingError::Internal(
                    InternalError::ChannelClosed,
                ));
            }
        };

        let _data = tokio::spawn(process_received_packet(
            packet,
            p_sender.clone(),
            fec_table.clone(),
        ));
    }
    // deserialize

    // send to packetizer
}

/// Processes a received packet through the full receive pipeline.
///
/// Steps:
/// 1. Process serialized packet (validate headers, extract metadata)
/// 2. Retrieve encryption key for the session
/// 3. Decrypt packet data
/// 4. Deserialize into typed PacketWrapper
async fn process_received_packet(
    received_packet: ReceivedPacket,
    sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
    fec_table: Arc<HashMap<Batch, HashSet<FecPacket>>>,
) -> Result<(), PacketProcessingError> {
    let processed = match process_serialized(received_packet) {
        Ok(packet) => packet,
        Err(err) => todo!("error handling"),
    };

    let key = get_key_from_session(processed.packet_id.session_id)
        .expect("literal value returned as Some");
    let decrypted_data = decrypt(processed.data, key);

    let processed = ProcessedPacket {
        packet_id: processed.packet_id,
        packet_type: processed.packet_type,
        data: decrypted_data,
        duplicate_count: processed.duplicate_count,
    };

    let packet = match deserialize(processed) {
        Ok(packet) => packet,
        Err(err) => todo!("error handling"),
    };

    match packet {
        PacketWrapper::DataPacket(pack) => {
            let batch = Batch {
                batch_id: pack.packet_type_batch_id.1,
                batch_size: pack.fec_info.batch_size,
            };
            // TODO: fix this
            fec_table
                .entry(batch)
                .or_insert(HashSet::from([pack.into()]))
        }
        _ => todo!(),
    };

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

    Ok(())
}

/// Retrieves the encryption key for a given session.
/// Currently returns a placeholder key - will be implemented with proper key management.
fn get_key_from_session(session_id: SessionId) -> Option<u128> {
    Some(34215909873652376164537433124 as u128)
}

/// Decrypts packet data using the provided key.
/// Currently a no-op placeholder - encryption implementation pending.
fn decrypt(packet: Vec<u8>, key: u128) -> Vec<u8> {
    _ = key;
    packet
}

/// Processes raw serialized packet data into a ProcessedPacket.
///
/// Validates and extracts:
/// - Protocol version (checks compatibility)
/// - Options flags
/// - Packet type
/// - Session ID
///
/// Performs header size validation based on packet type.
fn process_serialized(packet: ReceivedPacket) -> Result<ProcessedPacket, PacketProcessingError> {
    if packet.data.len() < 5 {
        return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
    }

    let packet_version = Version::from_bytes(
        packet.data[0..=1]
            .try_into()
            .expect("already guranteed size"),
    );

    if !packet_version.is_compatible() {
        return Err(PacketProcessingError::IncompatibleVersion(packet_version));
    }

    // bytes 2,3 are opts, not needed

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

    let session_id = SessionId::deserialize(&packet.data[6..14])
        .ok_or(PacketProcessingError::FailedToDeserialize)?;

    Ok(ProcessedPacket {
        packet_id: PacketId {
            timestamp: 0,
            session_id,
        },
        packet_type,
        data: packet.data,
        duplicate_count: 1,
    })
}

/// Deserializes a ProcessedPacket into a typed PacketWrapper.
///
/// Resizes the data buffer to match expected packet size before deserialization.
/// Different packet types require different buffer sizes based on their structure.
fn deserialize(mut packet: ProcessedPacket) -> Result<PacketWrapper, PacketProcessingError> {
    match packet.packet_type {
        PacketType::Data | PacketType::Parity => {
            packet
                .data
                .resize(DataPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);

            let packet = DataPacket::deserialize(&packet.data[..])
                .ok_or(PacketProcessingError::FailedToDeserialize)?;

            Ok(PacketWrapper::DataPacket(packet))
        }
        PacketType::Control => {
            unimplemented!();
            packet
                .data
                .resize(ControlPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            todo!("fix deserialization");
            //let Ok(deserialized) = wincode::deserialize::<ControlPacket>(&packet.data) else {
            //    return Err(PacketProcessingError::FailedToDeserialize);
            //};

            //Ok(PacketWrapper::ControlPacket(deserialized))
        }
        PacketType::Ack => {
            packet.data.resize(AckPacket::HEADER_SIZE, 0);

            let packet = AckPacket::deserialize(&packet.data[..])
                .ok_or(PacketProcessingError::FailedToDeserialize)?;

            Ok(PacketWrapper::AckPacket(packet))
        }

        // TODO: implement the rest after adding the packets
        _ => panic!("Havent taken care of this yet"),
    }
}
