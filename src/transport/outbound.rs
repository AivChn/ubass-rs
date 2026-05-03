use crate::packet_processor::types::ProcessedPacket;
use crate::prelude::*;
use crate::transport::types::OutboundReceiver;
use std::{mem, sync::Arc, vec};
use tokio::{
    net::UdpSocket,
    time::{Duration, Instant, timeout},
};
use tracing::{debug, error, instrument};

use super::types::{BUFFER_TIMEOUT, MAX_CONCURRENT_SENDS, MAX_PACKET_BUFFER_SIZE, OutboundSockets};

#[instrument]
pub async fn init(mut receiver: OutboundReceiver) -> ErrResult {
    // set up handle monitor
    let monitor = Arc::from(HandleMonitor::default());
    HandleMonitor::init(monitor.clone());

    // set up socket buffer
    let Ok(mut sockets) = OutboundSockets::new().await else {
        return Err(TransportError::FailedToBind.into());
    };

    // packet buffer
    let mut buffer = Vec::with_capacity(MAX_PACKET_BUFFER_SIZE);

    debug!("listening for packets to send...");
    loop {
        let start_time = Instant::now();

        while start_time.elapsed() < Duration::from_millis(BUFFER_TIMEOUT)
            && buffer.len() < MAX_PACKET_BUFFER_SIZE
        {
            #[allow(clippy::cast_possible_truncation)]
            let remaining = BUFFER_TIMEOUT - start_time.elapsed().as_millis() as u64;

            let data = match timeout(Duration::from_millis(remaining), receiver.recv()).await {
                // timeout ended
                Err(_) => break,
                // channel closed
                Ok(None) => {
                    error!("Channel closed on receiver side for Outbound Transport");
                    return Err(ChannelError::ChannelClosed(
                        PipeDirection::Outbound,
                        Layer::Transport,
                    )
                    .into());
                }
                // close pipeline
                Ok(Some(TransportMessage::Close)) => {
                    if !buffer.is_empty() {
                        monitor
                            .dispatch(send_packets(buffer.drain(..).collect(), sockets.retrieve()));
                    }
                    monitor.flush().await;
                    return Ok(());
                }
                // get data
                Ok(Some(TransportMessage::SendPacket(data))) => data,
            };

            buffer.push(data);
        }

        while monitor.size().await > MAX_CONCURRENT_SENDS {}

        #[allow(clippy::cast_possible_truncation)]
        let _ = sockets
            .update(start_time.elapsed().as_millis() as u64)
            .await;

        if buffer.is_empty() {
            continue;
        }

        #[cfg(debug_assertions)]
        if buffer.len() == 1 {
            debug!(
                "single packet being sent: {:?}",
                str::from_utf8(&buffer[0].data).unwrap_or(&format!("RAW: {:?}", &buffer[0].data))
            );
        }

        debug!("sending packet buffer with {} packets", buffer.len());
        #[allow(clippy::drain_collect)]
        monitor.dispatch(send_packets(buffer.drain(..).collect(), sockets.retrieve()));
    }
}

async fn send_packets(buffer: Box<[ProcessedPacket]>, socket: Arc<UdpSocket>) {
    for packet in buffer {
        debug_assert!(
            packet.duplicate_count != 0,
            "Invariant broken while sending packets: \
                a packet had a duplicate count of 0 ({packet:?})"
        );
        for _ in 0..packet.duplicate_count {
            let _ = socket.send_to(&packet.data, packet.dest_addr).await;
        }
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod test {
    use core::net;
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        sync::atomic::AtomicU16,
    };

    use tokio::{net::UdpSocket, task::JoinHandle};

    use crate::{
        error::ErrResult,
        manager::packets::PacketType,
        packet_processor::types::{OutboundSender, ProcessedPacket},
        transport::{outbound, types::OutboundReceiver},
        utils::{TransportMessage, messages},
    };

    static PORT: AtomicU16 = AtomicU16::new(43000);

    fn next_port() -> u16 {
        PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn prepare_init() -> (OutboundSender, JoinHandle<ErrResult>) {
        let (sender, receiver): (OutboundSender, _) = tokio::sync::mpsc::channel(1);
        (sender, tokio::spawn(outbound::init(receiver)))
    }

    #[tokio::test]
    async fn send_packet() {
        let port = next_port();
        let message = b"Hello World!";
        let receive = async move {
            let socket = UdpSocket::bind(format!("127.0.0.1:{port}")).await.unwrap();
            let mut buf = vec![0; 64];
            let read = socket.recv(&mut buf).await.unwrap();
            if &buf[..read] == message {
                Ok(())
            } else {
                Err(buf)
            }
        };

        let (sender, handle) = prepare_init();
        let packet = ProcessedPacket {
            dest_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
            packet_type: PacketType::Data,
            data: Vec::from(message),
            duplicate_count: 1,
        };

        sender.send(TransportMessage::SendPacket(packet)).await;
        let result = receive.await;
        dbg!(&result);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn graceful_close() {
        let (sender, handle) = prepare_init();
        sender.send(TransportMessage::Close).await;
        assert!(handle.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn send_multiple() {
        let port = next_port();
        let single_message = b"Hello World!";
        let messages = [single_message; 10];

        let receive = tokio::spawn(async move {
            let socket = UdpSocket::bind(format!("127.0.0.1:{port}")).await.unwrap();
            let mut buf = vec![0; 64];
            for _ in 0..10 {
                let read = socket.recv(&mut buf).await.unwrap();
                if &buf[..read] != single_message {
                    return Err(buf);
                }
            }
            Ok(())
        });

        let (sender, handle) = prepare_init();
        let packets: Vec<_> = messages
            .iter()
            .map(|message| ProcessedPacket {
                dest_addr: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
                packet_type: PacketType::Data,
                data: Vec::from(*message),
                duplicate_count: 1,
            })
            .collect();

        for packet in packets {
            sender.send(TransportMessage::SendPacket(packet)).await;
        }
        let result = receive.await.unwrap();
        dbg!(&result);
        assert!(result.is_ok());
    }
}
