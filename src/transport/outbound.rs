use crate::prelude::*;
use crate::{
    dispatch,
    manager::types::AddressSessionMonitor,
    packet_processor::types::{PacketId, ProcessedPacket, TransportMessage},
    packetizer::types::SessionId,
};
use std::sync::LazyLock;
use std::{collections::HashMap, sync::Arc, vec};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant, timeout},
};

use super::send_to_processing_layer;
use super::types::{
    BUFFER_TIMEOUT, MAX_CONCURRENT_SENDS, MAX_PACKET_BUFFER_SIZE, OutboundSockets, ReceivedPacket,
    SendablePacket,
};

static ADRESS_TABLE: LazyLock<AddressSessionMonitor> =
    LazyLock::new(AddressSessionMonitor::default);

pub async fn init(
    sender: Sender<Result<ReceivedPacket>>,
    mut receiver: Receiver<TransportMessage>,
) -> (Receiver<TransportMessage>, ErrResult) {
    let monitor = Arc::from(HandleMonitor::default());
    HandleMonitor::init(monitor.clone()).await;
    let Ok(mut sockets) = OutboundSockets::new().await else {
        return (receiver, Err(TransportError::FailedToBind.into()));
    };
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
                    return (
                        receiver,
                        Err(ChannelError::ChannelClosed(PipeDirection::Outbound).into()),
                    );
                }
                // close pipeline
                Ok(Some(TransportMessage::Close)) => {
                    monitor.flush().await;
                    return (receiver, Ok(()));
                }
                // get data
                Ok(Some(TransportMessage::Data(data))) => data,
            };

            buffer.extend(data);
        }

        #[allow(clippy::cast_possible_truncation)]
        let _ = sockets
            .update(start_time.elapsed().as_millis() as u64)
            .await;
        dispatch!(
            distribute_send_to_session(buffer, sockets.retrieve(), sender.clone()),
            monitor
        );

        buffer = vec![];
    }
}

/// Sends a batch of packets, multiplexed across sessions.
///
/// This function groups packets by session token, then sends to each session
/// concurrently. A single ephemeral socket is used for all outbound traffic.
///
/// # Arguments
///
/// * `buffer` - Packets to send, potentially to multiple different sessions
///
/// # Returns
///
/// * `Ok(())` - All packets sent successfully
/// * `Err(FailedToBind)` - Could not create outbound socket
/// * `Err(CouldNotSend)` - Some packets failed; contains their IDs
async fn distribute_send_to_session(
    buffer: Vec<ProcessedPacket>,
    socket: Arc<UdpSocket>,
    sender: Sender<Result<ReceivedPacket>>,
) {
    let mut sessions: HashMap<SessionId, Vec<SendablePacket>> = HashMap::new();
    for packet in buffer {
        let tok = packet.packet_id.session_id;
        let converted_packet: SendablePacket = SendablePacket::from(packet);
        sessions.entry(tok).or_default().push(converted_packet);
    }

    let mut futures = Vec::new();
    for (session, buffer) in sessions {
        futures.push(send_to(&socket, session, buffer));
    }

    let results = futures::future::join_all(futures).await;
    let errors: Vec<_> = results
        .into_iter()
        .filter_map(std::result::Result::err)
        .collect();

    if errors.is_empty() {
        return;
    }

    let could_not_send_error = Err(TransportError::CouldNotSend(
        errors
            .into_iter()
            .flat_map(|es| {
                // TODO: replace this with actual handling
                if let TransportError::CouldNotSend(val) = es.try_into().unwrap() {
                    val
                } else {
                    Vec::new()
                }
            })
            .collect::<Vec<PacketId>>(),
    )
    .into());

    _ = send_to_processing_layer(sender, could_not_send_error).await;
}

/// Sends all packets for a single session to its destination.
///
/// Each packet is sent `duplicate_count` times to provide redundancy
/// for unreliable networks (useful for control packets).
///
/// # Arguments
///
/// * `socket` - The UDP socket to send from
/// * `session_token` - Identifies the destination (decoded via [`get_addr`])
/// * `buffer` - Packets to send to this session
///
/// # Returns
///
/// * `Ok(())` - All packets sent successfully
/// * `Err(CouldNotSend)` - Some packets failed; contains their IDs
async fn send_to(
    socket: &UdpSocket,
    session_token: SessionId,
    buffer: Vec<SendablePacket>,
) -> ErrResult {
    let mut errors: Vec<PacketId> = vec![];

    for packet in buffer {
        for _ in 0..packet.duplicate_count {
            if socket
                .send_to(&packet.data, ADRESS_TABLE.get_addr(session_token))
                .await
                .is_err()
            {
                errors.push(packet.id);
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CouldNotSend(errors).into())
    }
}
