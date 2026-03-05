#![allow(unused_variables)]

use std::sync::Arc;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::{dispatch, prelude::*};

use super::types::*;

pub async fn init(
    OutboundChannels {
        t_sender,
        p_sender,
        mut p_receiver,
    }: OutboundChannels,
) -> (Receiver<PacketProcessingMessage>, ErrResult) {
    let monitor = Arc::from(HandleMonitor::new());
    tokio::spawn(HandleMonitor::init(monitor.clone()));

    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = p_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return (
                p_receiver,
                Err(ChannelError::ChannelClosed(Outbound).into()),
            );
        }

        let mut packets = Vec::with_capacity(received);

        for msg in buffer {
            let packet = match msg {
                PacketProcessingMessage::Close => return (p_receiver, Ok(())),
                PacketProcessingMessage::SendPacket(packet_wrapper) => packet_wrapper,
            };
            packets.push(packet);
        }

        dispatch!(
            handle_received(packets.into(), p_sender.clone(), t_sender.clone()),
            monitor
        );
    }
}

async fn handle_received(
    buffer: Box<[PacketWrapper]>,
    p_sender: Sender<Result<PacketWrapper>>,
    t_sender: Sender<TransportMessage>,
) {
    // TODO: implement this!!!
}
