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

mod inbound;
mod outbound;
pub mod types;

use crate::InternalError;
use tokio::sync::mpsc::Sender;

use types::*;

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
    InboundChannels { receiver, sender }: InboundChannels,
) -> Result<(), TransportError> {
    let mut recv_handle = tokio::spawn(inbound::init(port, sender.clone()));
    let mut send_handle = tokio::spawn(outbound::init(sender.clone(), receiver));

    'supervisor: loop {
        _ = tokio::select! {
            res = &mut recv_handle, if !recv_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TransportError::Internal(InternalError::TaskFailed));
                };

                match result {
                    Err(TransportError::RecvFailedTooManyTimes) |
                    Err(TransportError::FailedToBind) |
                    Err(TransportError::CouldNotSend(_)) => recv_handle = tokio::spawn(inbound::init(port, sender.clone())),
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
                    Err((TransportError::RecvFailedTooManyTimes, returned_receiver))=> send_handle = tokio::spawn(outbound::init(sender.clone(), returned_receiver)),
                    Err((TransportError::CouldNotSend(packets), returned_receiver)) => {
                        if sender.send(Err(TransportError::CouldNotSend(packets))).await.is_err() {
                            break 'supervisor Err(TransportError::Internal(InternalError::ChannelClosed));
                        }
                        send_handle = tokio::spawn(outbound::init(sender.clone(), returned_receiver));
                    },
                    Err((TransportError::Internal(internal), _)) => {recv_handle.abort(); break 'supervisor Err(TransportError::Internal(internal));},
                    Ok(()) => { recv_handle.abort(); break 'supervisor Ok(())},
                }
            }
        };
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
