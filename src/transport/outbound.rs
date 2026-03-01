use crate::{
    InternalError,
    packet_processor::types::{PacketId, ProcessedPacket, TransportSendMessage},
    packetizer::types::SessionId,
    utils,
};
use std::{collections::HashMap, fmt::Result, vec};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant, timeout},
};

use super::send_to_processing_layer;
use super::types::*;

/// Manages outbound packet transmission from the packet processor.
///
/// This function receives send commands via the channel, spawns send tasks,
/// and periodically collects results. It batches operations in 25ms windows
/// for efficiency.
///
/// # Arguments
///
/// * `receiver` - Channel for receiving [`TransportSendMessage`] commands
///
/// # Returns
///
/// * `Ok(())` - Graceful shutdown (`Close` message received)
/// * `Err((TransportError, Receiver))` - Error occurred; receiver is returned
///   so the supervisor can restart with the same channel (preserves connection)
///
/// # Batching Behavior
///
/// Messages are processed in 25ms batches:
/// 1. Spawn send tasks as `Data` messages arrive
/// 2. After 25ms, join all tasks and collect any errors
/// 3. If errors occurred, return them (supervisor may restart)
/// 4. Otherwise, start a new batch
pub async fn init(
    sender: Sender<Result<ReceivedPacket, TransportError>>,
    mut receiver: Receiver<TransportSendMessage>,
) -> Result<(), (TransportError, Receiver<TransportSendMessage>)> {
    let monitor = utils::HandleMonitor::new();
    let mut batch_count: u8 = 0;

    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        return Err((TransportError::FailedToBind, receiver));
    };

    loop {
        let now = Instant::now();

        if batch_count == 255 {
            batch_count = 0;
        }

        while now.elapsed() < Duration::from_millis(25)
            && monitor.size().await <= MAX_CONCURRENT_SENDS
        {
            let remaining = Duration::from_millis(25) - now.elapsed();
            let message = match timeout(remaining, receiver.recv()).await {
                // timeout reached
                Err(_) => break,
                // Channel closed
                Ok(None) => {
                    return Err((
                        TransportError::Internal(InternalError::ChannelFailed),
                        receiver,
                    ));
                }
                Ok(Some(msg)) => msg,
            };

            match message {
                TransportSendMessage::Data(buffer) => {
                    crate::dispatch!(distribute_send_to_session(buffer, sender.clone()), monitor);
                }
                TransportSendMessage::Close => {
                    monitor.flush().await;
                    return Ok(());
                }
            }
        }
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
    sender: Sender<Result<ReceivedPacket, TransportError>>,
) -> () {
    let mut sessions: HashMap<SessionId, Vec<SendablePacket>> = HashMap::new();
    for packet in buffer {
        let tok = packet.packet_id.session_id;
        let converted_packet: SendablePacket = SendablePacket::from(packet);
        sessions.entry(tok).or_default().push(converted_packet);
    }

    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        send_to_processing_layer(sender, Err(TransportError::FailedToBind)).await;
        return;
    };

    let mut futures: Vec<_> = Vec::new();
    for (session, buffer) in sessions {
        futures.push(send_to(&socket, session, buffer));
    }

    let results = futures::future::join_all(futures).await;
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();

    if !errors.is_empty() {
        send_to_processing_layer(
            sender,
            Err(TransportError::CouldNotSend(
                errors
                    .iter()
                    .flat_map(|es| {
                        if let TransportError::CouldNotSend(val) = es {
                            val.clone()
                        } else {
                            Vec::new()
                        }
                    })
                    .collect::<Vec<PacketId>>(),
            )),
        )
        .await;
    }
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
) -> Result<(), TransportError> {
    let mut errors: Vec<PacketId> = vec![];

    for packet in buffer {
        for _ in 0..packet.duplicate_count {
            if socket
                .send_to(&packet.data, get_addr(session_token))
                .await
                .is_err()
            {
                errors.push(packet.id);
            };
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CouldNotSend(errors))
    }
}

/// Decodes a session token to a destination address string.
///
/// **Note:** This is a placeholder implementation that only decodes the port
/// and assumes localhost. Will be replaced with proper session management.
pub fn get_addr(session_token: SessionId) -> String {
    let port = session_token.0 / (12 * 100_000_012);
    format!("127.0.0.1:{port}")
}
