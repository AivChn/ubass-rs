use crate::{
    packet_processor::serialize::PacketSerialize,
    packetizer::types::{AckPacket, DataPacket, PacketWrapper},
};
use tokio::sync::mpsc::{Receiver, Sender};

use super::types::*;

/// Send pipeline: handles outgoing packets from packetizer to transport.
///
/// Processing steps (planned):
/// 1. Wait for packet from packetizer
/// 2. Serialize packet
/// 3. Save copy for parity derivation
/// 4. Encrypt packet data
/// 5. Send to transport layer
/// 6. If final packet in batch:
///    - Derive parity packets
///    - Send parity packets
///    - Calculate new batch size for adaptive FEC
pub async fn init(
    OutboundChannels {
        t_sender,
        p_sender,
        p_receiver,
    }: OutboundChannels,
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
/// Converts a PacketWrapper into a ProcessedPacket ready for transmission.
///
/// Serializes the packet, extracts metadata, and prepares it for the transport layer.
/// Different packet types have different duplicate count defaults based on their importance.
fn process_packet(packet: PacketWrapper) -> ProcessedPacket {
    match packet {
        PacketWrapper::DataPacket(pack) => {
            // TODO: handle serialization error
            let mut data = [0u8; DataPacket::HEADER_SIZE + 1400];

            if !pack.serialize(&mut data[..]) {
                panic!("Have not handled this yet");
            }

            let size = pack.payload_length as usize + DataPacket::HEADER_SIZE;
            let data = data[..size].to_vec();

            ProcessedPacket {
                packet_id: PacketId {
                    timestamp: pack.timestamp_ms,
                    session_id: pack.session_id,
                },
                packet_type: pack.packet_type_batch_id.0,
                data,
                duplicate_count: 1,
            }
        }
        PacketWrapper::AckPacket(pack) => {
            // TODO: handle serialization error
            let mut data = [0u8; AckPacket::HEADER_SIZE];

            if !pack.serialize(&mut data[..]) {
                panic!("Have not handled this yet");
            }

            let data = data.to_vec();

            ProcessedPacket {
                packet_id: PacketId {
                    timestamp: pack.timestamp_ms,
                    session_id: pack.session_id,
                },
                packet_type: pack.packet_type,
                data,
                duplicate_count: 5,
            }
        }
        PacketWrapper::ControlPacket(pack) => {
            unimplemented!();
            // TODO: handle serialization error
            //let data = wincode::serialize(&pack).expect("I didnt handle this yet")
            //    [..pack.payload_length as usize + ControlPacket::HEADER_SIZE]
            //    .to_vec();

            //ProcessedPacket {
            //    packet_id: PacketId {
            //        timestamp: pack.timestamp_ms,
            //        session_id: pack.session_id,
            //    },
            //    packet_type: pack.packet_type,
            //    data,
            //    duplicate_count: 3,
            //}
        }
    }
}

/// Serializes a PacketWrapper into raw bytes.
/// Handles different packet types appropriately.
fn serialize_packet(wrapped_packet: PacketWrapper) -> Option<Vec<u8>> {
    match wrapped_packet {
        PacketWrapper::DataPacket(packet) => {
            let mut data = [0u8; DataPacket::HEADER_SIZE + 1400];
            if !packet.serialize(&mut data[..]) {
                None
            } else {
                Some(Vec::from(&data[..packet.payload_length as usize]))
            }
        }
        PacketWrapper::ControlPacket(packet) => {
            todo!("change serialization");
            // if let Ok(serialized) = wincode::serialize(&packet) {
            //     Some(
            //         serialized[..packet.payload_length as usize + ControlPacket::HEADER_SIZE]
            //             .to_vec(),
            //     )
            // } else {
            //     None
            // }
        }
        PacketWrapper::AckPacket(packet) => {
            let mut data = [0u8; AckPacket::HEADER_SIZE];
            if !packet.serialize(&mut data[..]) {
                None
            } else {
                Some(Vec::from(&data))
            }
        }
    }
}
