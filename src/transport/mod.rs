mod inbound;
mod outbound;
pub mod types;

use crate::{prelude::*, transport::types::InboundSender};

use types::TransportChannels;

/// initialize the transport layer
///
/// # Errors
///
/// this function returns `Ok(())` upon graceful shutdown
/// `Err(TaskFailed)` if one of the task handles fail
/// and any error propegated from the inbound and outbound ends of the pipeline
pub async fn init(
    port: u16,
    TransportChannels { receiver, sender }: TransportChannels,
) -> ErrResult {
    let mut recv_handle = tokio::spawn(inbound::init(port, sender.clone()));
    let mut send_handle = tokio::spawn(outbound::init(receiver));

    tokio::select! {
        res = &mut recv_handle, if !recv_handle.is_finished() => {
            match res {
                Ok(result) => result,
                Err(_) => Err(TaskError::TaskFailed.into()),
            }
        },
        res = &mut send_handle, if !send_handle.is_finished() => {
            match res {
                Ok(result) => result,
                Err(_) => Err(TaskError::TaskFailed.into()),
            }
        }
    }
}

async fn send_to_processing_layer(
    sender: InboundSender,
    res: Result<PacketProcessingMessage>,
) -> ErrResult {
    if sender.is_closed() {
        return Err(ChannelError::ChannelClosed(Inbound).into());
    }

    sender
        .send(res)
        .await
        .map_err(|_| ChannelError::ChannelFailed(Inbound).into())
}
