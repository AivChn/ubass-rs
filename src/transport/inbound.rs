use std::sync::Arc;

use crate::prelude::*;
use crate::transport::types::InboundSender;

use tokio::net::UdpSocket;
use tokio::select;
use tokio::sync::Notify;
use tracing::{debug, error, instrument, warn};

use super::send_to_processing_layer;
use super::types::{MAX_PACKET_SIZE, ReceivedPacket};

const MAX_ALLOWED_FAILS: u32 = 10;

#[instrument(skip_all)]
pub async fn init(socket: Arc<UdpSocket>, sender: InboundSender, signal: Arc<Notify>) -> ErrResult {
    let mut fail_count = 0u32;

    debug!("listening...");
    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];

        let addr = loop {
            let res = select! {
                res = socket.recv_from(&mut buffer) => {
                    res
                }
                _ = signal.notified() => {
                    _ = sender.send(Ok(PacketProcessingMessage::Closed)).await;
                    return Ok(());
                }
            };

            if let Ok((read, addr)) = res {
                fail_count = 0;
                buffer.truncate(read);
                break addr;
            }

            warn!("failed to receive {fail_count} times.");
            fail_count += 1;

            if fail_count >= MAX_ALLOWED_FAILS {
                error!("failed to receive too many times");
                return Err(TransportError::RecvFailedTooManyTimes.into());
            }
        };

        #[cfg(test)]
        debug!("got packet from {} size {}", addr, buffer.len());

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

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod test {
    use std::sync::{Arc, atomic::AtomicU16};

    use tokio::{net::UdpSocket, sync::Notify, task::JoinHandle};

    use crate::{
        error::{ChannelError, ErrResult, Error},
        packet_processor::types::InboundReceiver,
        transport::{
            bind_listen_socket, inbound,
            types::{InboundSender, ReceivedPacket},
        },
        utils::PacketProcessingMessage,
    };

    static PORT: AtomicU16 = AtomicU16::new(42000);

    fn next_port() -> u16 {
        PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn init_inbound(socket: Arc<UdpSocket>) -> (InboundReceiver, JoinHandle<ErrResult>) {
        let (sender, receiver): (InboundSender, _) = tokio::sync::mpsc::channel(1);

        let signal = Arc::new(Notify::new());
        (
            receiver,
            tokio::spawn(inbound::init(socket, sender, signal)),
        )
    }

    #[tokio::test]
    async fn get_packet() {
        let port = next_port();
        let socket = Arc::new(bind_listen_socket(port).unwrap());
        let (mut receiver, handle) = init_inbound(socket);
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert!(
            socket
                .send_to(b"Hello World!", format!("127.0.0.1:{port}"))
                .await
                .is_ok()
        );
        let packet = receiver.recv().await.unwrap();
        dbg!(&packet);
        let packet = packet.unwrap();
        let PacketProcessingMessage::ReceivedPacket(ReceivedPacket { src_addr, data }) = packet
        else {
            unreachable!();
        };

        assert_eq!(src_addr, socket.local_addr().unwrap());
        assert_eq!(data, Vec::from(b"Hello World!"));

        handle.abort();
    }

    #[tokio::test]
    async fn error_on_channel_close() {
        let port = next_port();
        let socket = Arc::new(bind_listen_socket(port).unwrap());
        let (mut receiver, handle) = init_inbound(socket);
        receiver.close();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert!(
            socket
                .send_to(b"Hello World!", format!("127.0.0.1:{port}"))
                .await
                .is_ok()
        );

        assert!(matches!(
            handle.await.unwrap(),
            Err(Error::Channel(ChannelError::ChannelClosed(
                crate::error::PipeDirection::Inbound,
                _
            )))
        ));
    }
}
