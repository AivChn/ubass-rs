use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::prelude::*;
use crate::transport::types::InboundSender;
use socket2::{Domain, Protocol, SockAddrStorage, Socket, Type};

use tokio::net::UdpSocket;

use super::send_to_processing_layer;
use super::types::{MAX_PACKET_SIZE, ReceivedPacket};

const MAX_ALLOWED_FAILS: u32 = 10;

pub async fn init(port: u16, sender: InboundSender) -> ErrResult {
    debug_assert!(
        !matches!(port, 1..=1024),
        "Invariant broken while initializing inbound transport: \
             system port was used ({port})"
    );

    let Ok(socket) = Socket::new(Domain::IPV4, Type::DGRAM, None) else {
        _ = send_to_processing_layer(sender, Err(TransportError::FailedToBind.into())).await;
        return Err(TransportError::FailedToBind.into());
    };
    assert!(socket.set_reuse_address(true).is_ok());
    assert!(socket.set_reuse_port(true).is_ok());
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
    let addr = socket2::SockAddr::from(addr);

    if let Err(e) = socket.bind(&addr) {
        _ = send_to_processing_layer(sender, Err(TransportError::FailedToBind.into())).await;
        return Err(TransportError::FailedToBind.into());
    }

    let std_socket: std::net::UdpSocket = socket.into();
    std_socket.set_nonblocking(true);
    let Ok(socket) = UdpSocket::from_std(std_socket) else {
        _ = send_to_processing_layer(sender, Err(TransportError::FailedToBind.into())).await;
        return Err(TransportError::FailedToBind.into());
    };

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

#[cfg(test)]
mod test {
    use std::{sync::atomic::AtomicU16, time::Duration};

    use aes_gcm_siv::aead::generic_array::typenum::type_operators;
    use tokio::{net::UdpSocket, task::JoinHandle};

    use crate::{
        DEFAULT_PORT,
        error::{ChannelError, ErrResult, Error},
        packet_processor::types::InboundReceiver,
        transport::{
            inbound,
            types::{InboundSender, OutboundReceiver, ReceivedPacket},
        },
    };

    static PORT: AtomicU16 = AtomicU16::new(42000);

    fn next_port() -> u16 {
        PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    async fn init_inbound(port: u16) -> (InboundReceiver, JoinHandle<ErrResult>) {
        let (sender, mut receiver): (InboundSender, _) = tokio::sync::mpsc::channel(1);

        (receiver, tokio::spawn(inbound::init(port, sender)))
    }

    #[tokio::test]
    async fn get_packet() {
        let port = next_port();
        let (mut receiver, handle) = init_inbound(port).await;
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
        let ReceivedPacket { src_addr, data } = match packet {
            crate::utils::PacketProcessingMessage::ReceivedPacket(received_packet) => {
                received_packet
            }
            _ => unreachable!(),
        };

        assert_eq!(src_addr, socket.local_addr().unwrap());
        assert_eq!(data, Vec::from(b"Hello World!"));

        handle.abort();
    }

    #[tokio::test]
    async fn invalid_port() {
        let port = 1;
        let (_, handle) = init_inbound(port).await;
        assert!(handle.await.is_err());
    }

    #[tokio::test]
    async fn error_on_channel_close() {
        let port = next_port();
        let (mut receiver, handle) = init_inbound(port).await;
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
                crate::error::PipeDirection::Inbound
            )))
        ));
    }
}
