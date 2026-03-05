#![allow(unused_variables)]

use crate::prelude::*;

use super::types::*;

use tokio::sync::mpsc::Sender;

pub async fn init(
    InboundChannels {
        mut t_receiver,
        p_sender,
    }: InboundChannels,
) -> ErrResult {
    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = t_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return Err(ChannelError::ChannelClosed(Inbound).into());
        }
    }
}

async fn handle_messages(
    buffer: Box<[Result<ReceivedPacket>]>,
    sender: Sender<Result<PacketWrapper>>,
) {
    // TODO: Finish this
    for packet in buffer {
        // TODO: Erorr handling
        let packet = packet.expect("Erorr handling");
    }
}
