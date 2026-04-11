mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
mod routines;
mod state;
pub mod types;

pub use routines::endpoints::*;

use std::{
    net::SocketAddr,
    sync::{OnceLock, mpsc as std_mpsc},
    thread::JoinHandle,
};

use crate::{
    DEFAULT_PORT,
    api::ApiErrors,
    lock_read, lock_write,
    manager::{
        self,
        packets::{
            AckPacket, AppRejectErrorPacket, ControlType, HelloPacket, HostControlType,
            IncompatibleVersionPacket, Options, PacketFingerprint, PacketType, SessionId, Version,
        },
        state::{EncryptionWindow, Port, SessionStateFlag, SessionStateFlags, SessionStates},
    },
    packet_processor::{self},
    prelude::*,
    transport::{self, types::TransportChannels},
};

use aes_gcm_siv::{Aes256GcmSiv, KeyInit};
use tokio::{runtime::Builder as RuntimeBuilder, sync::mpsc, time::Instant};

pub use state::{AppId, EncryptionMonitor, FingerprintMonitor, PendingAckMonitor};
use types::*;

/// random number, might change
// TODO: put some thought into this number
const CHANNEL_BUFFER_SIZE: usize = 256;

pub static STATE: OnceLock<SessionStates> = OnceLock::new();

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

pub async fn init(port: u16, app_id: AppId) -> core::result::Result<(), ApiErrors> {
    // try to create receiving socket
    std::net::UdpSocket::bind(format!("0.0.0.0:{port}"))
        .map_err(|_| ApiErrors::PortAlreadyInUse(port))?;

    let (manager_to_processor, processor_from_manager): (manager::OutboundSender, _) =
        mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (processor_to_manager, manager_from_processor): (
        packet_processor::types::InboundSender,
        _,
    ) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    let (transport_handle, processor_handle) =
        setup_layers(port, processor_to_manager, processor_from_manager).await?;

    STATE.set(SessionStates::new(
        Port::new(port),
        app_id,
        transport_handle,
        processor_handle,
    ));
    PROTOCOL_EPOCH.set(Instant::now());

    // TODO: implement selecting logic for manager layer
    let inbound_handle = tokio::spawn(inbound::init(manager_from_processor));
    // let outbound_handle = ...

    // loop
    // select

    Ok(())
}

async fn setup_layers(
    port: u16,
    processor_to_manager: packet_processor::types::InboundSender,
    processor_from_manager: packet_processor::types::OutboundReceiver,
) -> core::result::Result<(JoinHandle<()>, JoinHandle<()>), ApiErrors> {
    // create all the channels for the layers
    let (transport_to_processor, processor_from_transport): (transport::types::InboundSender, _) =
        mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (processor_to_transport, transport_from_processor): (
        packet_processor::types::OutboundSender,
        _,
    ) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    // ============= packet_processor =======================
    // create psuedo oneshot channel to get errors without disrupting the thread if succeeded
    let (pos, por): (std_mpsc::SyncSender<core::result::Result<(), ApiErrors>>, _) =
        std_mpsc::sync_channel(1);

    // initialize the processor layer
    let processor_handle = std::thread::spawn(move || {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .thread_name("Packet Processor")
            .build()
            .map_err(ApiErrors::FailedToBuildRuntime);

        match runtime {
            Err(e) => _ = pos.send(Err(e)),
            Ok(runtime) => {
                pos.send(Ok(()));
                runtime.block_on(async {
                    packet_processor::init(
                        processor_from_manager,
                        processor_to_manager,
                        processor_from_transport,
                        processor_to_transport,
                        encryption_monitor(),
                        fingerprint_monitor(),
                        pending_ack_monitor(),
                    )
                    .await;
                })
            }
        };
    });

    // wait on response from channel and return if error
    por.recv().expect(
        "Invariant broken while receiving on sync oneshot channel for packet processor:\
                            Channel closed before sending",
    )?;

    // ============= transport =======================
    let (tos, tor): (std_mpsc::SyncSender<core::result::Result<(), ApiErrors>>, _) =
        std_mpsc::sync_channel(1);

    // initialize the transport layer
    let transport_handle = std::thread::spawn(move || {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .thread_name("Transport")
            .build()
            .map_err(ApiErrors::FailedToBuildRuntime);

        match runtime {
            Err(e) => _ = tos.send(Err(e)),
            Ok(runtime) => {
                tos.send(Ok(()));
                runtime.block_on(async {
                    transport::init(
                        port,
                        TransportChannels {
                            receiver: transport_from_processor,
                            sender: transport_to_processor,
                        },
                    )
                    .await
                });
            }
        };
    });

    tor.recv().expect(
        "Invariant broken while receiving on sync oneshot channel for transport:\
                            Channel closed before sending",
    )?;

    Ok((transport_handle, processor_handle))
}
