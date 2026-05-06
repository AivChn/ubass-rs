mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
mod routines;
pub mod state;
pub mod types;

use std::sync::{OnceLock, atomic::Ordering, mpsc as std_mpsc};

use crate::{
    lock_read,
    manager::{
        self,
        packets::SessionId,
        state::{Port, ProtocolState},
    },
    packet_processor::{self, types::PacketProcessorChannels},
    prelude::*,
    transport::{self, types::TransportChannels},
};

use tokio::{
    runtime::Builder as RuntimeBuilder,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

pub use routines::endpoints;
pub use state::{AppId, EncryptionMonitor, FingerprintMonitor, PendingAckMonitor};
use types::*;

/// random number, might change
// TODO: put some thought into this number
const INBOUND_CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const CHANNEL_BUFFER_SIZE: usize = 256;

pub static STATE: OnceLock<ProtocolState> = OnceLock::new();

#[macro_export]
macro_rules! get_state {
    () => {
        STATE.get().expect("State accessed before protocol open")
    };
}

#[inline]
pub fn encryption_monitor() -> EncryptionMonitor {
    EncryptionMonitor::new(&get_state!().encryption)
}

#[inline]
pub fn fingerprint_monitor() -> FingerprintMonitor {
    FingerprintMonitor::new(&get_state!().fingerprints)
}

#[inline]
pub fn pending_ack_monitor() -> PendingAckMonitor {
    PendingAckMonitor::new(&get_state!().ack)
}

/// Function to initialize the manager layer and all following layers
///
/// # Errors
/// - `PortAlreadyInUse` if the port is already used
/// - `ThreadFailed` if any of the threads have failed
pub async fn init(
    port: u16,
    app_id: AppId,
    manager_to_api: ManagerToApi,
    manager_from_api: ManagerFromApi,
) -> core::result::Result<(), ApiErrors> {
    // try to create receiving socket
    std::net::UdpSocket::bind(format!("0.0.0.0:{port}"))
        .map_err(|_| ApiErrors::PortAlreadyInUse(port))?;

    let (manager_to_processor, processor_from_manager): (manager::ManagerToProcessor, _) =
        mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (processor_to_manager, manager_from_processor): (
        packet_processor::types::InboundSender,
        _,
    ) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    _ = STATE.set(ProtocolState::new(
        Port::new(port),
        app_id,
        manager_to_processor.clone(),
    ));
    _ = PROTOCOL_EPOCH.set(Instant::now());

    let (transport_handle, processor_handle) =
        setup_layers(port, processor_to_manager, processor_from_manager)
            .await
            .map_err(|_| ApiErrors::FailedToOpen)?;

    get_state!()
        .set_handles(transport_handle, processor_handle)
        .await;

    let mut inbound_handle = tokio::spawn(inbound::init(
        manager_from_processor,
        manager_to_processor.clone(),
        manager_to_api,
    ));

    let mut outbound_handle = tokio::spawn(outbound::init(
        manager_from_api,
        manager_to_processor.clone(),
    ));

    // TODO: flush pending acks before closing
    tokio::select! {
        res = &mut inbound_handle => {
            // no way to signal outbound — API holds the sender side
            outbound_handle.abort();
            match res {
                Ok(res) => res.map_err(|_| ApiErrors::ThreadFailed("Manager inbound")),
                Err(_) => Err(ApiErrors::ThreadFailed("Manager inbound")),
            }
        },
        res = &mut outbound_handle => {
            // close cascades downstream and back up via ManagerMessage::Closed
            // timeout in case inbound is slow to receive the cascade
            _ = tokio::time::timeout(INBOUND_CLOSE_TIMEOUT, inbound_handle).await;
            match res {
                Ok(Ok(())) => {
                    get_state!().advertise_closed().await;
                    Ok(())
                }
                _ => Err(ApiErrors::ThreadFailed("Manager outbound")),
            }
        },
    }
}

pub async fn session_exists(session_id: SessionId) -> bool {
    lock_read!(get_state!().connections).contains_key(&session_id)
}

async fn setup_layers(
    port: u16,
    processor_to_manager: packet_processor::types::InboundSender,
    processor_from_manager: packet_processor::types::OutboundReceiver,
) -> core::result::Result<(JoinHandle<ErrResult>, JoinHandle<ErrResult>), Error> {
    // create all the channels for the layers
    let (transport_to_processor, processor_from_transport): (transport::types::InboundSender, _) =
        mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (processor_to_transport, transport_from_processor): (
        packet_processor::types::OutboundSender,
        _,
    ) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    // ============= packet_processor =======================
    // create psuedo oneshot channel to get errors without disrupting the thread if succeeded
    let (pos, por) = oneshot::channel::<ErrResult>();

    let processor_handle = tokio::spawn(packet_processor::init(
        PacketProcessorChannels {
            from_manager: processor_from_manager,
            to_manager: processor_to_manager,
            from_transport: processor_from_transport,
            to_transport: processor_to_transport,
        },
        encryption_monitor(),
        fingerprint_monitor(),
        pending_ack_monitor(),
        pos,
    ));

    por.await
        .map_err(|_| Error::Task(TaskError::TaskFailed))??;

    // ============= transport =======================
    let (tos, tor) = oneshot::channel::<ErrResult>();

    let transport_handle = tokio::spawn(transport::init(
        port,
        TransportChannels {
            receiver: transport_from_processor,
            sender: transport_to_processor,
        },
        tos,
    ));

    tor.await
        .map_err(|_| Error::Task(TaskError::TaskFailed))??;
    Ok((transport_handle, processor_handle))
}
