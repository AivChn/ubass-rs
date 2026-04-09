mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
mod routines;
mod state;
pub mod types;

use std::{
    cell::OnceCell,
    net::SocketAddr,
    sync::{LazyLock, OnceLock},
};

use crate::{
    DEFAULT_PORT, lock_read, lock_write,
    manager::{
        packets::{
            AckPacket, AppRejectErrorPacket, ControlType, HelloPacket, HostControlType,
            IncompatibleVersionPacket, OptionFlags, Options, Packet, PacketFingerprint, PacketType,
            PublicKey, SessionId, Version,
        },
        routines::*,
        state::{EncryptionTable, EncryptionWindow, GeneralStateTable, Port, SessionStates},
    },
    o_unwrap_or_return,
    packet_processor::fingerprint,
    prelude::*,
};

use aes_gcm_siv::{Aes256GcmSiv, KeyInit};
use tokio::{sync::RwLock, time::Instant};

pub use state::{AppId, EncryptionMonitor, FingerprintMonitor, PendingAckMonitor};
use types::*;

pub static STATE: OnceLock<SessionStates> = OnceLock::new();

#[macro_export]
macro_rules! get_state {
    () => {
        STATE.get().expect("State accessed before protocol open")
    };
}

pub fn init() {
    PROTOCOL_EPOCH.get_or_init(Instant::now);
}

pub fn open(port: Port, app_id: AppId) {
    STATE.set(SessionStates::new(port, app_id));
}

/// Routine to handle an incompatible version error, occuring during deserialization.
/// The function sends an `IncompatibleVersionPacket` to the source, doing a reasonable effort to
/// make sure the packet arrives by sending it to the default port as well.
// TODO: Finish this
async fn received_incompatible_version_error(
    version: Version,
    src_addr: SocketAddr,
    sender: OutboundSender,
) {
    let packet = Packet::IncompatibleVersion(Box::new(IncompatibleVersionPacket::packet()));

    if src_addr.port() != DEFAULT_PORT {
        let mut alternative_address = src_addr;
        alternative_address.set_port(DEFAULT_PORT);
        sender
            .send(PacketProcessingMessage::SendPacket(
                packet.clone().wrap(alternative_address),
            ))
            .await;
    }

    sender
        .send(PacketProcessingMessage::SendPacket(
            packet.clone().wrap(src_addr),
        ))
        .await;
}

async fn connect(address: SocketAddr, other_app_id: AppId, outbound_sender: OutboundSender) {
    let session_id = SessionId::generate();
    let (ephemeral_secret, public_key) = key_exchange::create();

    get_state!()
        .new_session(session_id, address, other_app_id)
        .await;

    get_state!().new_handshake(address, ephemeral_secret);

    send_hello_packet(address, session_id, public_key, outbound_sender).await;
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
    app_sender: AppSender,
) {
    // setting the address to the one saved in state
    src_addr.set_port(*packet.receiving_port);
    if lock_read!(get_state!().handshakes).contains_key(&src_addr) {
        received_hello_packet_as_initializer(packet, src_addr, outbound_sender, app_sender).await;
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
    mut src_addr: SocketAddr,
    outbound_sender: OutboundSender,
    app_sender: AppSender,
) {
    // ask for permission to communicate with the giveb app ID
    let (request, receiver) = OneShot::new(packet.app_id.clone());
    let wrapped = AppRequest::HelloAppId(request);
    _ = app_sender.send(wrapped).await;

    match receiver.recv().await {
        // send back app rejected error
        AppResponse::AppRejected(message) => {
            send_app_rejected_error_packet(
                src_addr,
                Options::none(),
                packet.proposed_session_id,
                PacketType::Host,
                ControlType::Host(HostControlType::Hello),
                &packet,
                message,
                outbound_sender,
            )
            .await;
        }

        // create session and send back hello packet
        AppResponse::AppApproved(host_app_id) => {
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
                .new_session(session_id, src_addr, packet.app_id)
                .await;

            // create encryption entry
            let (ephemeral_secret, public_key) = key_exchange::create();
            let key = key_exchange::get_shared_secret(ephemeral_secret, packet.public_key);
            lock_write!(get_state!().encryption).insert(
                session_id,
                EncryptionWindow::new(Aes256GcmSiv::new((&key).into())),
            );

            send_hello_packet(src_addr, session_id, public_key, outbound_sender).await;
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
    app_sender: AppSender,
) {
    // if the session ID is already used that means that there was overlap on both hosts on both
    // attempts. This astronomically unlikely but is possible, so it's accounted for by restarting.
    if get_state!()
        .session_exists(packet.proposed_session_id)
        .await
    {
        connect(src_addr, packet.app_id, outbound_sender).await;
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
    let ack = Box::new(AckPacket::new(
        Options::none(),
        packet.proposed_session_id,
        fingerprint,
    ));

    outbound_sender
        .send(PacketProcessingMessage::SendPacket(
            Packet::AckPacket(ack).wrap(src_addr),
        ))
        .await;
}
