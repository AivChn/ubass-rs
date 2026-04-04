use crate::prelude::*;
use crate::transport::types::OutboundReceiver;
use crate::{dispatch, packet_processor::types::ProcessedPacket};
use std::{sync::Arc, vec};
use tokio::{
    net::UdpSocket,
    time::{Duration, Instant, timeout},
};

use super::types::{BUFFER_TIMEOUT, MAX_CONCURRENT_SENDS, MAX_PACKET_BUFFER_SIZE, OutboundSockets};

pub async fn init(mut receiver: OutboundReceiver) -> ErrResult {
    // set up handle monitor
    let monitor = Arc::from(HandleMonitor::default());
    HandleMonitor::init(monitor.clone()).await;

    // set up socket buffer
    let Ok(mut sockets) = OutboundSockets::new().await else {
        return Err(TransportError::FailedToBind.into());
    };

    // packet buffer
    let mut buffer = vec![];

    loop {
        let start_time = Instant::now();

        while start_time.elapsed() < Duration::from_millis(BUFFER_TIMEOUT)
            && buffer.len() < MAX_PACKET_BUFFER_SIZE
            && monitor.size().await < MAX_CONCURRENT_SENDS
        {
            #[allow(clippy::cast_possible_truncation)]
            let remaining = BUFFER_TIMEOUT - start_time.elapsed().as_millis() as u64;

            let data = match timeout(Duration::from_millis(remaining), receiver.recv()).await {
                // timeout ended
                Err(_) => break,
                // channel closed
                Ok(None) => {
                    return Err(ChannelError::ChannelClosed(PipeDirection::Outbound).into());
                }
                // close pipeline
                Ok(Some(TransportMessage::Close)) => {
                    monitor.flush().await;
                    return Ok(());
                }
                // get data
                Ok(Some(TransportMessage::SendPacket(data))) => data,
            };

            buffer.push(data);
        }

        if monitor.size().await < MAX_CONCURRENT_SENDS {
            #[allow(clippy::cast_possible_truncation)]
            let _ = sockets
                .update(start_time.elapsed().as_millis() as u64)
                .await;
        }

        if buffer.is_empty() {
            continue;
        }

        dispatch!(send_packets(buffer, sockets.retrieve()) => monitor);

        buffer = vec![];
    }
}

async fn send_packets(buffer: Vec<ProcessedPacket>, socket: Arc<UdpSocket>) {
    for packet in buffer {
        for _ in 0..packet.duplicate_count {
            let _ = socket.send_to(&packet.data, packet.dest_addr).await;
        }
    }
}
