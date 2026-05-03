mod inbound;
mod outbound;
pub mod types;

use crate::{prelude::*, transport::types::InboundSender};

use tracing::{debug, error, instrument};
use types::TransportChannels;

/// initialize the transport layer
///
/// # Errors
///
/// this function returns `Ok(())` upon graceful shutdown
/// `Err(TaskFailed)` if one of the task handles fail
/// and any error propegated from the inbound and outbound ends of the pipeline
#[instrument]
pub async fn init(
    port: u16,
    TransportChannels { receiver, sender }: TransportChannels,
) -> ErrResult {
    let mut recv_handle = tokio::spawn(inbound::init(port, sender.clone()));
    let mut send_handle = tokio::spawn(outbound::init(receiver));
    debug!("initializing the transport layer");

    tokio::select! {
        res = &mut recv_handle, if !recv_handle.is_finished() => {
            if let Ok(res) = res {
                debug!("receive transport returned as a result of graceful shutdown");
                res
            } else {
                error!("receive task failed");
                Err(TaskError::TaskFailed.into())
            }
        },
        res = &mut send_handle, if !send_handle.is_finished() => {
            if let Ok(res) = res {
                debug!("send transport returned as a result of graceful shutdown");
                res
            } else {
                error!("send task failed");
                Err(TaskError::TaskFailed.into())
            }
        }
    }
}

async fn send_to_processing_layer(
    sender: InboundSender,
    res: Result<PacketProcessingMessage>,
) -> ErrResult {
    if sender.is_closed() {
        return Err(ChannelError::ChannelClosed(Inbound, Layer::Manager).into());
    }

    sender.send(res).await.map_err(|_| {
        error!("failed to send on Inbound channel from Transport to Packet Processor");
        ChannelError::ChannelFailed(Inbound, Layer::Transport).into()
    })
}
