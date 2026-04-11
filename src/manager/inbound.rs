use std::sync::Arc;

use crate::{
    dispatch,
    manager::{
        routines::{
            errors::{self, handle_errors},
            received,
        },
        types::{InboundReceiver, InboundSender, OutboundSender},
    },
    prelude::*,
};

pub async fn init(
    mut inbound_receiver: InboundReceiver,
    outbound_sender: OutboundSender,
    app_sender: InboundSender,
) -> ErrResult {
    let monitor = Arc::new(HandleMonitor::default());
    monitor.clone().init();

    loop {
        match inbound_receiver.recv().await {
            None => {
                return Err(Error::Channel(ChannelError::ChannelClosed(Inbound)));
            }
            Some(Err(error)) => {
                dispatch!(handle_errors(error, outbound_sender.clone()) => monitor);
                continue;
            }
            Some(Ok(ManagerMessage::Closed)) => {
                return Ok(());
            }
            Some(Ok(message)) => {
                dispatch!(handle_message(message, outbound_sender.clone(), app_sender.clone()) => monitor);
                continue;
            }
        };
    }
}

async fn handle_message(
    message: ManagerMessage,
    outbound_sender: OutboundSender,
    app_sender: InboundSender,
) {
    match message {
        ManagerMessage::Recovered(recoverd_packets) => {
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
            packets::Packet::TrackRequestPacket(track_request_packet) => todo!(),
            packets::Packet::DataPacket(data_packet) => todo!(),
            packets::Packet::ParityPacket(parity_packet) => todo!(),
            packets::Packet::AckPacket(ack_packet) => todo!(),
            packets::Packet::IncompatibleVersionPacket(incompatible_version_packet) => todo!(),
            packets::Packet::SessionDoesNotExistErrorPacket(
                session_does_not_exist_error_packet,
            ) => todo!(),
            packets::Packet::AppRejectErrorPacket(app_reject_error_packet) => todo!(),
            // TODO: future features
            packets::Packet::RetransmitPacket(retransmit_packet) => todo!(),
            packets::Packet::MetadataPacket(metadata_packet) => todo!(),
            packets::Packet::PlaybackStatusPacket(playback_status_packet) => todo!(),
            packets::Packet::UnexpectedPacketErrorPacket(unexpected_packet_error_packet) => todo!(),
        },
        ManagerMessage::Closed => unreachable!("This arm is handled in the `init` match"),
    }
}
