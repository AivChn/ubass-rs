use std::sync::Arc;

use crate::{
    manager::{
        routines::{
            errors::handle_errors,
            received::{
                self, received_close_session_packet, received_handshake_ack_packet,
                received_handshake_rejected_packet, received_incompatible_version_error,
                received_keep_alive_packet, received_parity_packet,
                received_playback_control_packet, received_retransmit_request,
                received_session_does_not_exist_error, received_track_reject_packet,
                received_unexpected_packet_error,
            },
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
            packets::Packet::IncompatibleVersionPacket(packet) => {
                received_incompatible_version_error(packet, packet_wrapper.addr).await;
            }
            packets::Packet::SessionDoesNotExistErrorPacket(packet) => {
                received_session_does_not_exist_error(packet).await;
            }
            packets::Packet::TrackRejectionPacket(packet) => {
                received_track_reject_packet(packet).await;
            }
            // TODO: future features
            packets::Packet::RetransmitPacket(retransmit_packet) => {
                received_retransmit_request(retransmit_packet, outbound_sender.clone()).await;
            }
            // TODO: metadata flow not wired yet — silently drop.
            packets::Packet::MetadataPacket(_metadata_packet) => {}
            packets::Packet::PlaybackControlPacket(playback_status_packet) => {
                received_playback_control_packet(playback_status_packet, outbound_sender.clone())
                    .await;
            }
            packets::Packet::UnexpectedPacketErrorPacket(packet) => {
                received_unexpected_packet_error(packet).await;
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
