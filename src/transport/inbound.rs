use std::error;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use crate::prelude::*;
use crate::transport::types::InboundSender;
use socket2::{Domain, Protocol, SockAddrStorage, Socket, Type};

use tokio::net::UdpSocket;
use tracing::{debug, error, info, warn};

use super::send_to_processing_layer;
use super::types::{MAX_PACKET_SIZE, ReceivedPacket};

const MAX_ALLOWED_FAILS: u32 = 10;

pub async fn init(socket: Arc<UdpSocket>, sender: InboundSender) -> ErrResult {
    let mut fail_count = 0u32;

    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];

        let addr = loop {
            if let Ok((read, addr)) = socket.recv_from(&mut buffer).await {
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
    use std::{
        net::SocketAddrV4,
        sync::{Arc, atomic::AtomicU16},
        time::Duration,
    };

    use aes_gcm_siv::aead::generic_array::typenum::type_operators;
    use tokio::{net::UdpSocket, task::JoinHandle};

    use crate::{
        DEFAULT_PORT,
        error::{ChannelError, ErrResult, Error},
        packet_processor::types::InboundReceiver,
        transport::{
            bind_listen_socket, inbound,
            types::{InboundSender, OutboundReceiver, ReceivedPacket},
        },
        utils::PacketProcessingMessage,
    };

    static PORT: AtomicU16 = AtomicU16::new(42000);

    fn next_port() -> u16 {
        PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn init_inbound(socket: Arc<UdpSocket>) -> (InboundReceiver, JoinHandle<ErrResult>) {
        let (sender, receiver): (InboundSender, _) = tokio::sync::mpsc::channel(1);

        (receiver, tokio::spawn(inbound::init(socket, sender)))
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
