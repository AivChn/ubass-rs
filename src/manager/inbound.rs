use std::sync::Arc;

use crate::{
    get_state,
    manager::{
        routines::{
            errors::{self, handle_errors},
            received::{
                self, received_close_session_packet, received_handshake_ack_packet,
                received_handshake_rejected_packet, received_keep_alive_packet,
                received_parity_packet, received_playback_control_packet,
            },
        },
        types::{ManagerFromProcessor, ManagerToApi, ManagerToProcessor},
    },
    packet_processor::fec::received,
    prelude::*,
};

pub async fn init(
    mut inbound_receiver: ManagerFromProcessor,
    outbound_sender: ManagerToProcessor,
    app_sender: ManagerToApi,
) -> ErrResult {
    let monitor = Arc::new(HandleMonitor::default());

    loop {
        match inbound_receiver.recv().await {
            None => {
                return Err(ChannelError::ChannelClosed(Inbound, Layer::Manager).into());
            }
            Some(Err(error)) => monitor.dispatch(handle_errors(error, outbound_sender.clone())),
            Some(Ok(ManagerMessage::Closed)) => {
                monitor.flush().await;
                return Ok(());
            }
            Some(Ok(message)) => {
                monitor.dispatch(handle_message(
                    message,
                    outbound_sender.clone(),
                    app_sender.clone(),
                ));
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
            packets::Packet::ParityPacket(box parity_packet) => {
                received_parity_packet(parity_packet, outbound_sender.clone()).await;
            }
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
            packets::Packet::PlaybackStatusPacket(playback_status_packet) => {
                received_playback_control_packet(playback_status_packet, outbound_sender.clone())
                    .await;
            }
            packets::Packet::UnexpectedPacketErrorPacket(_unexpected_packet_error_packet) => {
                todo!()
            }
            packets::Packet::CloseSessionPacket(close_session_packet) => {
                received_close_session_packet(close_session_packet).await;
            }
            packets::Packet::HandshakeRejection(handshake_rejection) => {
                received_handshake_rejected_packet(handshake_rejection).await;
            }
            packets::Packet::KeepAlivePacket(keep_alive_packet) => {
                received_keep_alive_packet(keep_alive_packet).await;
            }
        },
        ManagerMessage::Closed => unreachable!("This arm is handled in the `init` match"),
    }
}
