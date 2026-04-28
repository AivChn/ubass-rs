use std::sync::Arc;

use crate::{
    manager::{
        routines::{
            errors::{self, handle_errors},
            received::{self, received_handshake_ack_packet},
        },
        types::{ManagerFromProcessor, ManagerToApi, ManagerToProcessor},
    },
    prelude::*,
};

pub async fn init(
    mut inbound_receiver: ManagerFromProcessor,
    outbound_sender: ManagerToProcessor,
    app_sender: ManagerToApi,
) -> ErrResult {
    let monitor = Arc::new(HandleMonitor::default());
    monitor.clone().init();

    loop {
        match inbound_receiver.recv().await {
            None => {
                return Err(ChannelError::ChannelClosed(Inbound).into());
            }
            Some(Err(error)) => {
                monitor
                    .dispatch(handle_errors(error, outbound_sender.clone()))
                    .await;
            }
            Some(Ok(ManagerMessage::Closed)) => {
                monitor.flush().await;
                return Ok(());
            }
            Some(Ok(message)) => {
                monitor
                    .dispatch(handle_message(
                        message,
                        outbound_sender.clone(),
                        app_sender.clone(),
                    ))
                    .await;
            }
        }
    }
}

async fn handle_message(
    message: ManagerMessage,
    outbound_sender: ManagerToProcessor,
    app_sender: ManagerToApi,
) {
    match message {
        ManagerMessage::Recovered(_recoverd_packets) => {
            todo!("recovered packets routine")
            // TODO: call recoverd routine
            // TODO: call data received routine
        }
        ManagerMessage::Packet(packet_wrapper) => match packet_wrapper.packet {
            packets::Packet::HelloPacket(hello_packet) => {
                received::received_hello_packet(
                    *hello_packet,
                    packet_wrapper.addr,
                    outbound_sender,
                    app_sender,
                )
                .await;
            }
            packets::Packet::DataPacket(data_packet) => {
                received::received_data_packet(*data_packet, outbound_sender).await;
            }
            packets::Packet::HandshakeAckPacket(handshake_ack_packet) => {
                received_handshake_ack_packet(handshake_ack_packet).await;
            }
            packets::Packet::TrackRequestPacket(track_request_packet) => {
                received::received_track_request_packet(track_request_packet).await;
            }
            packets::Packet::ParityPacket(_parity_packet) => todo!(),
            packets::Packet::AckPacket(ack_packet) => {
                received::received_ack_packet(ack_packet).await;
            }
            packets::Packet::IncompatibleVersionPacket(_incompatible_version_packet) => todo!(),
            packets::Packet::SessionDoesNotExistErrorPacket(
                _session_does_not_exist_error_packet,
            ) => todo!(),
            packets::Packet::AppRejectErrorPacket(_app_reject_error_packet) => todo!(),
            // TODO: future features
            packets::Packet::RetransmitPacket(_retransmit_packet) => todo!(),
            packets::Packet::MetadataPacket(_metadata_packet) => todo!(),
            packets::Packet::PlaybackStatusPacket(_playback_status_packet) => todo!(),
            packets::Packet::UnexpectedPacketErrorPacket(_unexpected_packet_error_packet) => {
                todo!()
            }
            packets::Packet::CloseSessionPacket(close_session_packet) => todo!(),
        },
        ManagerMessage::Closed => unreachable!("This arm is handled in the `init` match"),
    }
}
