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

use crate::prelude::*;

use tokio::sync::mpsc::Sender;

use types::{InboundChannels, ReceivedPacket};

/// Initializes and supervises the transport layer.
///
/// This is the main entry point for the transport layer. It spawns two concurrent tasks
/// (recv and send) and supervises them in a loop, handling restarts on recoverable errors.
///
/// # Errors
///
/// `TaskError::TaskFailed`: returned if the task failed not by returning an error.
///
/// # Supervisor Behavior
///
/// The supervisor loop uses `tokio::select!` to monitor both tasks:
/// - Guards (`!is_finished()`) prevent polling already-completed handles
/// - On recoverable errors, the failed task is restarted
/// - On unrecoverable errors or graceful shutdown, both tasks are cleaned up
pub async fn init(port: u16, InboundChannels { receiver, sender }: InboundChannels) -> ErrResult {
    let mut recv_handle = tokio::spawn(inbound::init(port, sender.clone()));
    let mut send_handle = tokio::spawn(outbound::init(sender.clone(), receiver));

    'supervisor: loop {
        tokio::select! {
            res = &mut recv_handle, if !recv_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TaskError::TaskFailed.into());
                };

                let err = match result {
                    Ok(()) => {
                        recv_handle.abort();
                        break 'supervisor Ok(());
                    }
                    Err(err) => err,
                };

                match err.contents() {
                    ErrorContents::Transport(
                        TransportError::RecvFailedTooManyTimes |
                        TransportError::FailedToBind |
                        TransportError::CouldNotSend(_)) => recv_handle = tokio::spawn(inbound::init(port, sender.clone())),
                    _ => todo!("Finish the Error match for receive select branch"),
                }
            },
            res = &mut send_handle, if !send_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TaskError::TaskFailed.into());
                };

                let (receiver, result) = result;

                let err = match result {
                    Ok(()) => {
                        recv_handle.abort();
                        break 'supervisor Ok(());
                    }
                    Err(err) => err,
                };

                let ErrorContents::Transport(error) = err.consume_contents() else {
                    todo!();
                };

                match error {
                    TransportError::FailedToBind |
                    TransportError::RecvFailedTooManyTimes => send_handle = tokio::spawn(outbound::init(sender.clone(), receiver)),
                    TransportError::CouldNotSend(packets) => {
                        if sender.send(Err(TransportError::CouldNotSend(packets).into())).await.is_err() {
                            return Err(ChannelError::ChannelClosed(Outbound).into());
                        }
                        send_handle = tokio::spawn(outbound::init(sender.clone(), receiver));
                    },
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
    sender: Sender<Result<ReceivedPacket>>,
    res: Result<ReceivedPacket>,
) -> ErrResult {
    if sender.is_closed() {
        return Err(ChannelError::ChannelClosed(Inbound).into());
    }

    sender
        .send(res)
        .await
        .map_err(|_| ChannelError::ChannelFailed(Inbound).into())
}
