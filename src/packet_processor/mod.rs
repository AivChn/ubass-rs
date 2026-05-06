pub mod encryption;
mod inbound;
mod outbound;

pub mod fec;
pub mod fingerprint;
pub mod serialize;
pub mod types;

use crate::{
    manager::{EncryptionMonitor, FingerprintMonitor, PendingAckMonitor},
    packet_processor::types::{
        InboundReceiver, InboundSender, OutboundReceiver, OutboundSender, PacketProcessorChannels,
    },
    prelude::*,
};

use tokio::sync::oneshot;
use types::{InboundChannels, OutboundChannels};

/// initialize the packet processor
///
/// # Errors
///
/// propegates any error returned from the inbound or outbound ends of the pipline
/// returns `Ok(())` to indicate a gracefull shutdown
pub async fn init(
    PacketProcessorChannels {
        from_manager,
        to_manager,
        from_transport,
        to_transport,
    }: PacketProcessorChannels,
    encryption_monitor: EncryptionMonitor,
    fingerprint_monitor: FingerprintMonitor,
    pending_ack_monitor: PendingAckMonitor,
    signal: oneshot::Sender<ErrResult>,
) -> ErrResult {
    let mut inbound_handle = tokio::spawn(inbound::init(
        InboundChannels {
            from_transport,
            to_manager: to_manager.clone(),
        },
        encryption_monitor,
        fingerprint_monitor,
    ));
    let mut outbound_handle = tokio::spawn(outbound::init(
        OutboundChannels {
            to_transport: to_transport.clone(),
            to_manager: to_manager.clone(),
            from_manager,
        },
        encryption_monitor,
        pending_ack_monitor,
    ));
    _ = signal.send(Ok(()));

    tokio::select! {
        res = &mut inbound_handle => {
            match res {
                Err(_) => {
                    _ = to_transport.send(TransportMessage::Close).await;
                    _ = outbound_handle.await;
                    Err(TaskError::TaskFailed.into())
                },
                Ok(res) =>  {
                    _ = outbound_handle.await;
                    _ = to_transport.send(TransportMessage::Close).await;
                    res
                },
            }
        },
        res = &mut outbound_handle => {
            match res {
                Err(_) => {
                    _ = to_transport.send(TransportMessage::Close).await;
                    inbound_handle.abort();
                    Err(TaskError::TaskFailed.into())
                },
                Ok(res) =>  {
                    inbound_handle.abort();
                    _ = to_transport.send(TransportMessage::Close).await;
                    res
                },
            }
        }
    }
}
