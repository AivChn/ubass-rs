use std::net::SocketAddr;

use aes_gcm_siv::{Aes256GcmSiv, KeyInit};

use crate::{
    DEFAULT_PORT, get_state, lock_read, lock_write,
    manager::{
        STATE, connect, key_exchange,
        packets::*,
        state::{EncryptionWindow, SessionStateFlag, SessionStateFlags},
        types::{InboundSender, OutboundSender},
    },
    utils::{AppMessage, AppResponse, Flags, OneShot, SendPacket},
};

pub async fn received_data_packet(packet: DataPacket) {
    todo!()
}

/// Routine to handle the case of receiving any [`HelloPacket`].
/// This function will set the source address port to the one specified in the packet, and check if
/// there is an entry tied to it in the handshakes state table. Based on the result it will either
/// call [`received_hello_packet_as_initializer`] or [`received_hello_packet_as_receiver`]
///
/// # Panics
/// the two called functions panic on broken invariants, check their documentation for more information
pub async fn received_hello_packet(
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
pub async fn received_hello_packet_as_receiver(
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
pub async fn received_hello_packet_as_initializer(
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

/// Routine to handle an incompatible version error, occuring during deserialization.
/// The function sends an `IncompatibleVersionPacket` to the source, doing a reasonable effort to
/// make sure the packet arrives by sending it to the default port as well.
pub async fn received_incompatible_version_error(
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
