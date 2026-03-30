mod encryption;
mod fec;
mod inbound;
mod outbound;
pub mod serialize;
pub mod types;

use crate::{manager::types::EncryptionMonitor, prelude::*};

use tokio::sync::mpsc::{Receiver, Sender};

use types::{
    InboundChannels, OutboundChannels, PacketProcessingMessage, PacketWrapper, ReceivedPacket,
    TransportMessage,
};

// =================== PUBLIC FUNCTIONS =================================|

/// Initializes the packet processing layer and supervises send/recv tasks.
///
/// Spawns two concurrent tasks:
/// - recv: Handles incoming packets from transport layer, processes and forwards to packetizer
/// - send: Handles outgoing packets from packetizer, processes and forwards to transport
///
/// Acts as a supervisor, monitoring both tasks and handling failures.
/// If either task fails, the supervisor will abort the other and return an error.
///
/// # Errors
///
/// This function only errors on unrecoverable errors occuring from either the tasks returning an
/// error or failing to start.
/// `TaskError::TaskFailed`: returned if the tasks failed - not by returning an error.
/// TODO finish this
pub async fn init(
    p_receiver: Receiver<PacketProcessingMessage>,
    p_sender: Sender<Result<PacketWrapper>>,
    t_receiver: Receiver<Result<ReceivedPacket>>,
    t_sender: Sender<TransportMessage>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> ErrResult {
    let mut recv_handle = tokio::spawn(inbound::init(
        InboundChannels {
            t_receiver,
            p_sender: p_sender.clone(),
        },
        encryption_monitor,
    ));
    let mut send_handle = tokio::spawn(outbound::init(OutboundChannels {
        t_sender: t_sender.clone(),
        p_sender: p_sender.clone(),
        p_receiver,
    }));

    'supervisor: loop {
        _ = tokio::select! {
            res = &mut recv_handle, if !recv_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TaskError::TaskFailed.into());
                };

                match result {
                    // TODO: update error handling
                    Err(e) => Err::<(), _>(e),
                    Ok(()) => break 'supervisor Ok(()),
                }
            },
            res = &mut send_handle, if !send_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(TaskError::TaskFailed.into());
                };

                match result {
                    // TODO: update error handling
                    (_, Err(e)) => Err(e),
                    (_, Ok(())) => { recv_handle.abort(); break 'supervisor Ok(())},
                }
            }
        }
    }
}
