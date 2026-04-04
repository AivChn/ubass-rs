use crate::prelude::*;
use crate::transport::types::InboundSender;

use tokio::{net::UdpSocket, sync::mpsc::Sender};

use super::send_to_processing_layer;
use super::types::{MAX_PACKET_SIZE, ReceivedPacket};

pub async fn init(port: u16, sender: InboundSender) -> ErrResult {
    const MAX_ALLOWED_FAILS: u32 = 10;
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|_| TransportError::FailedToBind)?;
    let mut fail_count = 0u32;

    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];

        let addr = loop {
            if let Ok((read, addr)) = socket.recv_from(&mut buffer).await {
                fail_count = 0;
                buffer.truncate(read);
                break addr;
            }

            fail_count += 1;

            if fail_count >= MAX_ALLOWED_FAILS {
                return Err(TransportError::RecvFailedTooManyTimes.into());
            }
        };

        let packet = ReceivedPacket {
            src_addr: addr,
            data: buffer,
        };

        send_to_processing_layer(
            sender.clone(),
            Ok(PacketProcessingMessage::ReceivedPacket(packet)),
        )
        .await?;
    }
}
