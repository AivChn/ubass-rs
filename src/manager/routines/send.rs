use std::net::SocketAddr;

use crate::manager::packets::{ControlType, HelloPacket, Packet, PacketFingerprint, PacketType};
use crate::packet_processor::fingerprint::{Fingerprint, Headers};
use crate::{get_state, prelude::*};

use crate::manager::{STATE, packets::*, types::OutboundSender};

pub async fn send_hello_packet(
    address: SocketAddr,
    session_id: SessionId,
    public_key: impl Into<PublicKey>,
    sender: OutboundSender,
) {
    let hello_packet = Box::new(HelloPacket::new(
        Options::none(),
        session_id,
        public_key.into(),
        get_state!().app_id(),
        get_state!().port(),
    ));

    sender
        .send(PacketProcessingMessage::SendPacket(
            Packet::HelloPacket(hello_packet).wrap(address),
        ))
        .await;
}

pub async fn send_app_rejected_error_packet<T>(
    address: SocketAddr,
    opts: Options,
    session_id: SessionId,
    packet_type: PacketType,
    control_type: ControlType,
    fingerprint: &T,
    message: String,
    sender: OutboundSender,
) where
    T: Fingerprint,
    for<'a> &'a T: Into<PacketFingerprint>,
{
    let rejected_packet = Box::new(AppRejectErrorPacket::new(
        opts,
        session_id,
        packet_type,
        control_type,
        fingerprint.into(),
        message,
    ));

    sender
        .send(PacketProcessingMessage::SendPacket(
            Packet::AppRejectErrorPacket(rejected_packet).wrap(address),
        ))
        .await;
}
