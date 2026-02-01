//! Transport Layer for UBASS (UDP Audio Streaming System)
//!
//! This module implements the lowest layer of the protocol stack, responsible for:
//! - Binding and managing UDP sockets for sending and receiving packets
//! - Multiplexing packets across multiple sessions (identified by session tokens)
//! - Providing redundancy through configurable duplicate packet transmission
//! - Error recovery with automatic task restart on recoverable failures
//!
//! # Architecture
//!
//! The transport layer runs two concurrent tasks supervised by [`init()`]:
//! - **recv task**: Listens for incoming UDP packets and forwards them up to the packet processor
//! - **send task**: Receives outbound packets from the packet processor and transmits them via UDP
//!
//! Communication with the packet processor layer occurs through two MPSC channels:
//! - `Receiver<TransportSendMessage>`: Commands and packets to send (from processor)
//! - `Sender<Result<ReceivedPacket, TransportError>>`: Received packets and errors (to processor)
//!
//! # Error Handling
//!
//! Errors are categorized as:
//! - **Recoverable** (`FailedToBind`, `CouldNotSend`): Task is restarted automatically
//! - **Unrecoverable** (`Internal`): Supervisor shuts down both tasks and propagates error

//#![allow(warnings)]

use crate::{
    InternalError,
    packet_processor::{PacketId, ProcessedPacket, TransportSendMessage},
    packetizer::SessionId,
};
use std::{collections::HashMap, net::SocketAddr, vec};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant, timeout},
};

/// Maximum UDP packet size in bytes (1452).
const MAX_PACKET_SIZE: usize = 1452;

const MAX_CONCURRENT_SENDS: usize = 128;

/// Packet ready for UDP transmission with metadata for error reporting and redundancy.
#[derive(Debug, Clone)]
pub struct SendablePacket {
    pub id: PacketId,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct ReceivedPacket {
    pub data: Vec<u8>,
}

impl ReceivedPacket {
    fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl From<ProcessedPacket> for SendablePacket {
    fn from(value: ProcessedPacket) -> Self {
        Self {
            id: value.packet_id,
            data: value.data,
            duplicate_count: value.duplicate_count,
        }
    }
}

/// Errors that can occur in the transport layer.
///
/// These errors are used both internally for task supervision and externally
/// to communicate failures to the packet processor layer.
#[derive(Debug, Clone)]
pub enum TransportError {
    /// One or more packets failed to send. Contains the IDs of failed packets
    /// so they can be retried or reported by upper layers.
    CouldNotSend(Vec<PacketId>),
    /// Failed to bind a UDP socket to the requested address/port.
    FailedToBind,
    RecvFailedTooManyTimes,
    /// Internal protocol error (task failure, channel issues).
    /// Wraps [`InternalError`] for ergonomic error propagation.
    Internal(InternalError),
}

impl PartialEq for TransportError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CouldNotSend(_), Self::CouldNotSend(_)) => true,
            (Self::FailedToBind, Self::FailedToBind) => true,
            _ => false,
        }
    }
}

/// Initializes and supervises the transport layer.
///
/// This is the main entry point for the transport layer. It spawns two concurrent tasks
/// (recv and send) and supervises them in a loop, handling restarts on recoverable errors.
///
/// # Arguments
///
/// * `port` - The UDP port to bind the listening socket to
/// * `receiver` - Channel for receiving send commands from the packet processor
/// * `sender` - Channel for forwarding received packets to the packet processor
///
/// # Returns
///
/// * `Ok(())` - Graceful shutdown (received `Close` message)
/// * `Err(TransportError)` - Unrecoverable error occurred
///
/// # Supervisor Behavior
///
/// The supervisor loop uses `tokio::select!` to monitor both tasks:
/// - Guards (`!is_finished()`) prevent polling already-completed handles
/// - On recoverable errors, the failed task is restarted
/// - On unrecoverable errors or graceful shutdown, both tasks are cleaned up
pub async fn init(
    port: u16,
    receiver: Receiver<TransportSendMessage>,
    sender: Sender<Result<ReceivedPacket, TransportError>>,
) -> Result<(), TransportError> {
    let mut recv_handle = tokio::spawn(recv(port, sender.clone()));
    let mut send_handle = tokio::spawn(send(sender.clone(), receiver));

    'supervisor: loop {
        _ = tokio::select! {
            res = &mut recv_handle, if !recv_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TransportError::Internal(InternalError::TaskFailed));
                };

                match result {
                    Err(TransportError::RecvFailedTooManyTimes) |
                    Err(TransportError::FailedToBind) |
                    Err(TransportError::CouldNotSend(_)) => recv_handle = tokio::spawn(recv(port, sender.clone())),
                    Err(TransportError::Internal(internal)) => {send_handle.abort(); break 'supervisor Err(TransportError::Internal(internal));},
                    Ok(()) => break 'supervisor Ok(()),
                }
            },
            res = &mut send_handle, if !send_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TransportError::Internal(InternalError::TaskFailed));
                };

                match result {
                    Err((TransportError::FailedToBind, returned_receiver)) |
                    Err((TransportError::RecvFailedTooManyTimes, returned_receiver))=> send_handle = tokio::spawn(send(sender.clone(), returned_receiver)),
                    Err((TransportError::CouldNotSend(packets), returned_receiver)) => {
                        if sender.send(Err(TransportError::CouldNotSend(packets))).await.is_err() {
                            break 'supervisor Err(TransportError::Internal(InternalError::ChannelClosed));
                        }
                        send_handle = tokio::spawn(send(sender.clone(), returned_receiver));
                    },
                    Err((TransportError::Internal(internal), _)) => {recv_handle.abort(); break 'supervisor Err(TransportError::Internal(internal));},
                    Ok(()) => { recv_handle.abort(); break 'supervisor Ok(())},
                }
            }
        };
    }
}

/// Listens for incoming UDP packets and forwards them to the packet processor.
///
/// This function runs an infinite loop, receiving packets and sending them up
/// through the provided channel. It only exits on unrecoverable errors.
///
/// # Arguments
///
/// * `port` - The UDP port to listen on (binds to `0.0.0.0:{port}`)
/// * `sender` - Channel to forward received packets to the packet processor
///
/// # Returns
///
/// * `Ok(())` - Never returns this in practice (loops forever)
/// * `Err(FailedToBind)` - Could not bind to the specified port
/// * `Err(Internal)` - Channel to packet processor closed
///
/// # Note
///
/// This function is designed to be forcefully aborted by the supervisor on shutdown,
/// as it has no mechanism for graceful termination.
async fn recv(
    port: u16,
    sender: Sender<Result<ReceivedPacket, TransportError>>,
) -> Result<(), TransportError> {
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|_| TransportError::FailedToBind)?;
    let mut fail_count = 0;

    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];

        let stripped_buffer = loop {
            match socket.recv(&mut buffer).await {
                Ok(read) => break &buffer[..read],
                Err(_) => {
                    if fail_count >= 25 {
                        return Err(TransportError::RecvFailedTooManyTimes);
                    } else {
                        fail_count += 1;
                    }
                }
            }
            if let Ok(read) = socket.recv(&mut buffer).await {
                fail_count = 0;
                break &buffer[..read];
            }
        };

        let packet = ReceivedPacket::new(stripped_buffer.to_vec());

        if let Err(intern) = send_to_processing_layer(sender.clone(), Ok(packet)).await {
            return Err(TransportError::Internal(intern));
        }
    }
}

/// Sends a received packet to the packet processor layer via channel.
///
/// This is a helper function that wraps channel send operations with
/// appropriate error handling.
///
/// # Arguments
///
/// * `sender` - The channel sender to the packet processor
/// * `res` - The packet to send
///
/// # Returns
///
/// * `Ok(())` - Packet successfully queued
/// * `Err(ChannelClosed)` - Channel was already closed (checked before send)
/// * `Err(ChannelFailed)` - Send operation failed
async fn send_to_processing_layer(
    sender: Sender<Result<ReceivedPacket, TransportError>>,
    res: Result<ReceivedPacket, TransportError>,
) -> Result<(), InternalError> {
    if sender.is_closed() {
        return Err(InternalError::ChannelClosed);
    }

    match res {
        Ok(packet) => {
            if sender.send(Ok(packet)).await.is_err() {
                return Err(InternalError::ChannelFailed);
            }
        }
        Err(err) => {
            if sender.send(Err(err)).await.is_err() {
                return Err(InternalError::ChannelFailed);
            }
        }
    }

    Ok(())
    //
}

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
async fn send(
    sender: Sender<Result<ReceivedPacket, TransportError>>,
    mut receiver: Receiver<TransportSendMessage>,
) -> Result<(), (TransportError, Receiver<TransportSendMessage>)> {
    loop {
        let mut tasks = Vec::with_capacity(MAX_CONCURRENT_SENDS);
        let now = Instant::now();

        while now.elapsed() < Duration::from_millis(25) && tasks.len() <= MAX_CONCURRENT_SENDS {
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
                    tasks.push(tokio::spawn(distribute_send_to_session(buffer)))
                }
                TransportSendMessage::Close => {
                    _ = futures::future::join_all(tasks).await;
                    return Ok(());
                }
            }
        }

        let results: Vec<_> = futures::future::join_all(tasks)
            .await
            .iter()
            .map(|res| res.as_ref().unwrap_or(&Ok(())))
            .filter(|res| res.is_err())
            .flat_map(|err| match err {
                Ok(()) => unreachable!(),
                Err(e) => match e {
                    TransportError::CouldNotSend(packet_id) => packet_id,
                    _ => unreachable!(),
                },
            })
            .map(|refr| *refr)
            .collect();

        if !results.is_empty() {
            if let Err(err) =
                send_to_processing_layer(sender.clone(), Err(TransportError::CouldNotSend(results)))
                    .await
            {
                return Err((TransportError::Internal(err), receiver));
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
async fn distribute_send_to_session(buffer: Vec<ProcessedPacket>) -> Result<(), TransportError> {
    let mut sessions: HashMap<SessionId, Vec<SendablePacket>> = HashMap::new();
    for packet in buffer {
        let tok = packet.packet_id.session_id;
        let converted_packet: SendablePacket = SendablePacket::from(packet);
        sessions.entry(tok).or_default().push(converted_packet);
    }

    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        return Err(TransportError::FailedToBind);
    };

    let mut futures: Vec<_> = Vec::new();
    for (session, buffer) in sessions {
        futures.push(send_to(&socket, session, buffer));
    }

    let results = futures::future::join_all(futures).await;
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();

    if errors.is_empty() {
        Ok(())
    } else {
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
        ))
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

/// Encodes a socket address into a session token.
///
/// **Note:** This is a placeholder implementation that only encodes the port,
/// ignoring the IP address. Will be replaced with proper session management.
pub fn get_session_token(addr: SocketAddr) -> u128 {
    (addr.port() as u128) * 12 * 100_000_012
}
