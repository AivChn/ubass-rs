use crate::prelude::*;
use crate::transport::types::OutboundReceiver;
use crate::{manager::packets::PacketType, packet_processor::types::ProcessedPacket};
use std::{mem, sync::Arc, vec};
use tokio::{
    net::UdpSocket,
    time::{Duration, Instant, timeout},
};
use tracing::{debug, error, instrument};

use super::types::{BUFFER_TIMEOUT, MAX_CONCURRENT_SENDS, MAX_PACKET_BUFFER_SIZE, OutboundSockets};

#[instrument]
pub async fn init(mut receiver: OutboundReceiver, listening_socket: Arc<UdpSocket>) -> ErrResult {
    // set up handle monitor
    let monitor = Arc::from(HandleMonitor::default());

    // set up socket buffer
    let Ok(mut sockets) = OutboundSockets::new().await else {
        error!("Failed to bind socket");
        return Err(TransportError::FailedToBind.into());
    };

    // packet buffer
    let mut buffer = Vec::with_capacity(MAX_PACKET_BUFFER_SIZE);

    #[cfg(test)]
    let mut total_received: usize = 0;

    debug!("listening for packets to send...");
    loop {
        let start_time = Instant::now();

        while start_time.elapsed() < Duration::from_millis(BUFFER_TIMEOUT)
            && buffer.len() < MAX_PACKET_BUFFER_SIZE
        {
            #[allow(clippy::cast_possible_truncation)]
            let remaining = BUFFER_TIMEOUT.saturating_sub(start_time.elapsed().as_millis() as u64);

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
                    #[cfg(test)]
                    debug!(
                        "outbound transport begins graceful shutdown: \
                        buffer={} total_received={}",
                        buffer.len(),
                        total_received
                    );

                    #[cfg(not(test))]
                    debug!("outbound transport begins graceful shutdown");

                    if !buffer.is_empty() {
                        monitor
                            .dispatch(send_packets(buffer.drain(..).collect(), sockets.retrieve()));
                    }
                    monitor.flush().await;
                    debug!("graceful shutdown done");
                    return Ok(());
                }
                // get data
                Ok(Some(TransportMessage::SendPacket(data))) => data,
            };

            #[cfg(test)]
            {
                total_received += 1;
            }

            if data.packet_type == PacketType::KeepAlive || data.packet_type == PacketType::Host {
                let copy = listening_socket.clone();
                monitor.dispatch(async move {
                    _ = copy.send_to(&data.data, data.dest_addr).await;
                });
            } else {
                buffer.push(data);
            }
        }

        while monitor.size() > MAX_CONCURRENT_SENDS {}

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
                "single {} packet being sent: {:?}",
                buffer[0].packet_type,
                str::from_utf8(&buffer[0].data).unwrap_or(&format!("RAW: {:?}", buffer[0].data))
            );
        }

        debug!("sending packet buffer with {} packets", buffer.len(),);
        #[allow(clippy::drain_collect)]
        monitor.dispatch(send_packets(buffer.drain(..).collect(), sockets.retrieve()));
    }
}

async fn send_packets(buffer: Box<[ProcessedPacket]>, socket: Arc<UdpSocket>) {
    let total = buffer.len();
    #[cfg(test)]
    let (mut sent, mut failed) = (0usize, 0usize);

    debug!("send_packets: starting batch of {total} packets");
    for packet in buffer {
        debug_assert!(
            packet.duplicate_count != 0,
            "Invariant broken while sending packets: \
                a packet had a duplicate count of 0 ({packet:?})"
        );
        for _ in 0..packet.duplicate_count {
            #[cfg(test)]
            match socket.send_to(&packet.data, packet.dest_addr).await {
                Ok(_) => sent += 1,
                Err(e) => {
                    error!("send_to failed: {e}");
                    failed += 1;
                }
            }

            #[cfg(not(test))]
            if let Err(e) = socket.send_to(&packet.data, packet.dest_addr).await {
                error!("send_to failed: {e}");
            }
        }
    }

    #[cfg(test)]
    debug!("send_packets: done — sent={sent} failed={failed} total={total}");
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod test {
    use core::net;
    use std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        sync::{Arc, atomic::AtomicU16},
    };

    use tokio::{net::UdpSocket, task::JoinHandle};

    use crate::{
        error::ErrResult,
        manager::packets::PacketType,
        packet_processor::types::{OutboundSender, ProcessedPacket},
        transport::{bind_listen_socket, outbound, types::OutboundReceiver},
        utils::{TransportMessage, messages},
    };

    static PORT: AtomicU16 = AtomicU16::new(43000);

    fn next_port() -> u16 {
        PORT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn prepare_init() -> (OutboundSender, JoinHandle<ErrResult>) {
        let socket = Arc::new(bind_listen_socket(next_port()).unwrap());
        let (sender, receiver): (OutboundSender, _) = tokio::sync::mpsc::channel(1);
        (sender, tokio::spawn(outbound::init(receiver, socket)))
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

        _ = sender.send(TransportMessage::SendPacket(packet)).await;
        let result = receive.await;
        dbg!(&result);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn graceful_close() {
        let (sender, handle) = prepare_init();
        _ = sender.send(TransportMessage::Close).await;
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
            _ = sender.send(TransportMessage::SendPacket(packet)).await;
        }
        let result = receive.await.unwrap();
        dbg!(&result);
        assert!(result.is_ok());
    }
}
