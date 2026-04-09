mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
mod routines;
mod state;
pub mod types;

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
const CHANNEL_BUFFER_SIZE: usize = 128;

pub static STATE: OnceLock<SessionStates> = OnceLock::new();

#[macro_export]
macro_rules! get_state {
    () => {
        STATE.get().expect("State accessed before protocol open")
    };
}

pub fn open(
    port: u16,
    app_id: AppId,
) -> core::result::Result<JoinHandle<core::result::Result<(), ApiErrors>>, ApiErrors> {
    let (mos, mor): (std_mpsc::SyncSender<core::result::Result<(), ApiErrors>>, _) =
        std_mpsc::sync_channel(1);

    // ============= manager =======================
    let manager_handle = std::thread::spawn(move || {
        let thread_name = "Manager";
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .thread_name(thread_name)
            .build()
            .map_err(ApiErrors::FailedToBuildRuntime);

        match runtime {
            Err(e) => {
                mos.send(Err(e));
                Err(ApiErrors::ThreadFailed(thread_name))
            }
            Ok(runtime) => {
                mos.send(Ok(()));
                runtime.block_on(async { init(port, app_id).await })
            }
        }
    });

    mor.recv().expect(
        "Invariant broken while receiving on sync oneshot channel for manager:\
                            Channel closed before sending",
    )?;

    Ok(manager_handle)
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

    Ok(())
}

/// Routine to handle an incompatible version error, occuring during deserialization.
/// The function sends an `IncompatibleVersionPacket` to the source, doing a reasonable effort to
/// make sure the packet arrives by sending it to the default port as well.
async fn received_incompatible_version_error(
    _version: Version,
    src_addr: SocketAddr,
    sender: OutboundSender,
) {
    let packet = IncompatibleVersionPacket::packet();

    if src_addr.port() != DEFAULT_PORT {
        let mut alternative_address = src_addr;
        alternative_address.set_port(DEFAULT_PORT);
        packet
            .clone()
            .send(sender.clone(), alternative_address)
            .await;
    }

    packet.send(sender, src_addr).await;
}

/// Root Routine for the [`API::connect`] endpoint.
/// This function creates a handshake entry and sends a [`HelloPacket`] to the given address,
/// assuming that is the receiving port for the host.
async fn connect(address: SocketAddr, outbound_sender: OutboundSender) {
    let session_id = SessionId::generate();
    let (ephemeral_secret, public_key) = key_exchange::create();

    get_state!().new_handshake(address, ephemeral_secret);

    HelloPacket::new(
        Options::none(),
        session_id,
        public_key.into(),
        get_state!().app_id(),
        get_state!().port(),
    )
    .send(outbound_sender, address)
    .await;
}

/// Routine to handle the case of receiving any [`HelloPacket`].
/// This function will set the source address port to the one specified in the packet, and check if
/// there is an entry tied to it in the handshakes state table. Based on the result it will either
/// call [`received_hello_packet_as_initializer`] or [`received_hello_packet_as_receiver`]
///
/// # Panics
/// the two called functions panic on broken invariants, check their documentation for more information
async fn received_hello_packet(
    packet: HelloPacket,
    mut src_addr: SocketAddr,
    outbound_sender: OutboundSender,
    app_sender: InboundSender,
) {
    // setting the address to the one saved in state
    src_addr.set_port(*packet.receiving_port);
    if lock_read!(get_state!().handshakes).contains_key(&src_addr) {
        received_hello_packet_as_initializer(packet, src_addr, outbound_sender).await;
    } else {
        received_hello_packet_as_receiver(packet, src_addr, outbound_sender, app_sender).await;
    }
}

/// Routine to handle the case of receiving a [`HelloPacket`] as receiver AKA without sending one first.
/// This function will send the app ID received to the app for verification, and will respond with
/// either an app rejection or a hello packet of its own.
///
/// # Invariants
/// 1. oneshot must send: any oneshot channel must be used before being dropped.
///
/// # Panics
/// This function calls [`AppResponseReceiver::recv()`] which will panic if the "oneshot must send"
/// invariant is broken
async fn received_hello_packet_as_receiver(
    packet: HelloPacket,
    src_addr: SocketAddr,
    outbound_sender: OutboundSender,
    app_sender: InboundSender,
) {
    // ask for permission to communicate with the giveb app ID
    let (request, receiver) = OneShot::new(packet.app_id.clone());
    let wrapped = AppMessage::HelloAppId(request);
    _ = app_sender.send(Ok(wrapped)).await;

    match receiver.recv().await {
        // send back app rejected error
        AppResponse::AppRejected(message) => {
            AppRejectErrorPacket::new(
                Options::none(),
                packet.proposed_session_id,
                PacketType::Host,
                ControlType::Host(HostControlType::Hello),
                PacketFingerprint::from(&packet),
                message,
            )
            .send(outbound_sender, src_addr)
            .await;
        }

        // create session and send back hello packet
        AppResponse::AppApproved => {
            // handle collisions
            let session_id = if get_state!()
                .session_exists(packet.proposed_session_id)
                .await
            {
                SessionId::generate()
            } else {
                packet.proposed_session_id
            };

            // create session
            get_state!()
                .new_session(
                    SessionStateFlags::construct(&[SessionStateFlag::Hanshake]),
                    session_id,
                    src_addr,
                    packet.app_id,
                )
                .await;

            // create encryption entry
            let (ephemeral_secret, public_key) = key_exchange::create();
            let key = key_exchange::get_shared_secret(ephemeral_secret, packet.public_key);
            lock_write!(get_state!().encryption).insert(
                session_id,
                EncryptionWindow::new(Aes256GcmSiv::new((&key).into())),
            );

            HelloPacket::new(
                Options::none(),
                session_id,
                public_key.into(),
                get_state!().app_id(),
                get_state!().port(),
            )
            .send(outbound_sender, src_addr)
            .await;
        }
    }
}

/// Routine to handle the case of receiving a [`HelloPacket`] as the intializer of the handshake.
/// This function will promote the handshake to a full session and send back an ack to complete the
/// handshake.
///
/// # Caveats
/// if the other host already has a session with the proposed session ID it will send back a different session ID.
/// If this host has a session with that session ID, handshake will be restarted completely.
/// This is **extremely unlikely**, since session ID is a randomly generated u64.
///
/// # Invariants
/// 1. handshake state must exist: if this function is called it is guaranteed that the address has
///    an entry in the handshakes state table.
///
/// # Panics
/// This function does not panic, but it is the only call site for
/// [`SessionStates::promote_handshake()`], which will panic if the "handshake state must exist"
/// invariant is broken.
async fn received_hello_packet_as_initializer(
    packet: HelloPacket,
    src_addr: SocketAddr,
    outbound_sender: OutboundSender,
) {
    // if the session ID is already used that means that there was overlap on both hosts on both
    // attempts. This astronomically unlikely but is possible, so it's accounted for by restarting.
    if get_state!()
        .session_exists(packet.proposed_session_id)
        .await
    {
        connect(src_addr, outbound_sender).await;
        return;
    }

    // get packet fingerprint before any partial moves
    let fingerprint = PacketFingerprint::from(&packet);

    let secret = get_state!()
        .promote_handshake(packet.proposed_session_id, src_addr, packet.app_id)
        .await;
    let key = key_exchange::get_shared_secret(secret, packet.public_key);
    let cipher = Aes256GcmSiv::new(&key.into());

    // insert cipher to state for any future encryption
    lock_write!(get_state!().encryption)
        .insert(packet.proposed_session_id, EncryptionWindow::new(cipher));

    // construct ack and send
    AckPacket::new(Options::none(), packet.proposed_session_id, fingerprint)
        .send(outbound_sender, src_addr)
        .await;
}
