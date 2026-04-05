mod encryption;
mod inbound;
mod outbound;

pub mod fec;
pub mod fingerprint;
pub mod serialize;
pub mod types;

use crate::{
    manager::{EncryptionMonitor, FingerprintMonitor, PendingAckMonitor},
    packet_processor::types::{InboundReceiver, InboundSender, OutboundReceiver, OutboundSender},
    prelude::*,
};

use types::{InboundChannels, OutboundChannels};
/// initialize the packet processor
///
/// # Errors
///
/// propegates any error returned from the inbound or outbound ends of the pipline
/// returns `Ok(())` to indicate a gracefull shutdown
pub async fn init(
    p_receiver: OutboundReceiver,
    p_sender: InboundSender,
    t_receiver: InboundReceiver,
    t_sender: OutboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
    pending_ack_monitor: &'static PendingAckMonitor<'_>,
) -> ErrResult {
    let mut inbound_handle = tokio::spawn(inbound::init(
        InboundChannels {
            t_receiver,
            p_sender: p_sender.clone(),
        },
        encryption_monitor,
        fingerprint_monitor,
    ));
    let mut outbound_handle = tokio::spawn(outbound::init(
        OutboundChannels {
            t_sender: t_sender.clone(),
            p_sender: p_sender.clone(),
            p_receiver,
        },
        encryption_monitor,
        pending_ack_monitor,
    ));

    tokio::select! {
        res = &mut inbound_handle, if !inbound_handle.is_finished() => {
            match res {
                Err(_) => Err(TaskError::TaskFailed.into()),
                Ok(res) =>  res,
            }
        },
        res = &mut outbound_handle, if !outbound_handle.is_finished() => {
            match res {
                Err(_) => Err(TaskError::TaskFailed.into()),
                Ok(res) => res,
            }
        }
    }
}
