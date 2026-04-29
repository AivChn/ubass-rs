use std::{net::SocketAddr, time::Duration};

use crate::{
    error::ConnectionError,
    lock,
    manager::state::{ConnectionStates, EstablishedState, SessionStates, StreamState},
    match_or_return,
    prelude::*,
    utils::PanicInDebug,
};
use aes_gcm_siv::{Aes256GcmSiv, KeyInit};
use tokio::sync::mpsc::{self, Receiver};

use crate::{
    DEFAULT_PORT, get_state, lock_read, lock_write,
    manager::{
        STATE, key_exchange,
        packets::*,
        state::EncryptionWindow,
        types::{ManagerToApi, ManagerToProcessor},
    },
    o_unwrap_or_return, r_unwrap_or_return,
    utils::{ApiMessage, AppResponse, ConnectionEvent, Flags, OneShot, SendPacket},
};

const BUFFERING_TIME_FOR_HANDSHAKE: u64 = 7;

pub async fn received_close_session_packet(packet: Box<CloseSessionPacket>) {
    {
        let lock = lock_read!(get_state!().connections);
        let session = o_unwrap_or_return!(lock.get(&packet.session_id));
        match session {
            ConnectionStates::Established(box EstablishedState {
                state: SessionStates::Streaming(StreamState { stream, .. }),
                ..
            }) => stream.send_modify(|m| m.closed = true),
            ConnectionStates::Established(box EstablishedState { connection, .. }) => {
                connection.send(ConnectionEvent::Closed);
            }
            ConnectionStates::Handshake { .. } => {}
        }
    }

    get_state!().close_session(packet.session_id);
}

pub async fn received_track_request_packet(packet: Box<TrackRequestPacket>) {
    let track_id = packet.payload.take();
    let sender = {
        let lock = lock_read!(get_state!().connections);
        o_unwrap_or_return!({
            if let Some(ConnectionStates::Established(box EstablishedState {
                connection, ..
            })) = lock.get(&packet.session_id)
            {
                Some(connection)
            } else {
                None
            }
        })
        .clone()
    };
    _ = sender
        .send(ConnectionEvent::TrackRequest(track_id.into()))
        .await;
}

pub async fn update_last_activity(session_id: SessionId, ts: Timestamp) {
    if let Some(ConnectionStates::Established(box EstablishedState { last_activity, .. })) =
        lock_read!(get_state!().connections).get(&session_id)
    {
        lock!(last_activity).update(ts.get());
    }
}

pub async fn received_handshake_rejected_packet(packet: Box<HandshakeRejection>) {
    match packet.reason {
        HandshakeRejectionReason::App => {
            if let Some(handshake) =
                lock_write!(get_state!().handshakes).remove(&packet.handshake_id)
            {
                let reason = if packet.payload.len() == 1 && packet.payload[0] == 0 {
                    None
                } else {
                    String::from_utf8(packet.payload.take()).ok()
                };
                handshake
                    .response
                    .send(Err(ConnectionError::PeerRejected(reason)));
            }
        }
        HandshakeRejectionReason::IdCollision => {
            if let Some(ConnectionStates::Handshake {
                ack_triggered_response,
                ..
            }) = lock_write!(get_state!().connections).remove(&packet.session_id)
            {
                ack_triggered_response.send(Err(ConnectionError::SessionIdCollided));
            }
        }
    }
}

pub async fn received_ack_packet(packet: Box<AckPacket>) {
    update_last_activity(packet.session_id, packet.timestamp).await;
    get_state!().ack.acknowledge(packet.fingerprint).await;
}

pub async fn received_data_packet(packet: DataPacket, outbound_sender: ManagerToProcessor) {
    get_state!()
        .global_handle_monitor
        .dispatch(update_last_activity(packet.session_id, packet.timestamp));

    if let ConnectionStates::Handshake { signal, .. } = o_unwrap_or_return!(
        lock_read!(get_state!().connections)
            .get(&packet.session_id)
            .panic_in_debug(&format!(
                "Invariant broken in `received_data_packet`: \
                got a packet for a session that does not exist, \
                this should not happen at this stage as it is handled earlier. {packet:?}"
            ))
    ) {
        let mut listener = signal.subscribe();
        _ = r_unwrap_or_return!(
            tokio::time::timeout(
                Duration::from_secs(BUFFERING_TIME_FOR_HANDSHAKE),
                listener.wait_for(|val| *val),
            )
            .await
        );
    }

    if packet.opts.contains(OptionFlags::RequireAck) {
        received_packet_that_requires_ack(packet.session_id, &packet, outbound_sender.clone())
            .await;
    }

    let fingerprint = PacketFingerprint::from(&packet);
    let session_id = packet.session_id;
    if o_unwrap_or_return!(lock_write!(get_state!().connections).get_mut(&packet.session_id))
        .received_data_packet(packet)
        .is_err_and(|e| matches!(e, Error::StateMismatch { .. }))
    {
        UnexpectedPacketErrorPacket::unexpected(
            Options::none(),
            session_id,
            PacketType::Data,
            SecondaryType::none(),
            fingerprint,
        )
        .send(
            outbound_sender,
            o_unwrap_or_return!(get_state!().connections.address(session_id).await),
        )
        .await;
    }
}

/// Routine to handle the case of receiving any [`HelloPacket`].
///
/// This function will set the source address port to the one specified in the packet, and check if
/// there is an entry tied to it in the handshakes state table. Based on the result it will either
/// call [`received_hello_packet_as_initializer`] or [`received_hello_packet_as_receiver`]
///
/// # Panics
/// the two called functions panic on broken invariants, check their documentation for more information
pub async fn received_hello_packet(
    packet: HelloPacket,
    mut src_addr: SocketAddr,
    outbound_sender: ManagerToProcessor,
    app_sender: ManagerToApi,
) {
    // setting the address to the one saved in state
    src_addr.set_port(*packet.receiving_port);
    if lock_read!(get_state!().handshakes).contains_key(&packet.handshake_id) {
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
    outbound_sender: ManagerToProcessor,
    app_sender: ManagerToApi,
) {
    // ask for permission to communicate with the given app ID
    let (app_id_request, app_id_response) = OneShot::<_, AppResponse>::new(packet.app_id.clone());
    let (handshake_request, handshake_response) = OneShot::<
        _,
        core::result::Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>,
    >::new(());
    let wrapped = ApiMessage::IncommingConncetion {
        request: app_id_request,
        response: handshake_response,
        peer_address: src_addr,
    };
    _ = app_sender.send(Ok(wrapped)).await;

    match r_unwrap_or_return!(app_id_response.recv().await) {
        // send back app rejected error
        AppResponse::AppRejected(message) => {
            Box::new(HandshakeRejection::new(
                Options::none(),
                packet.proposed_session_id,
                HandshakeRejectionReason::App,
                packet.handshake_id,
                PayloadField::from(message),
            ))
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

            // create session in handshake state
            get_state!()
                .new_session(
                    session_id,
                    handshake_request.response,
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
                Options::construct(&[OptionFlags::RequireAck]),
                session_id,
                packet.handshake_id,
                public_key,
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
/// [`SessionStates::promote_handshake`], which will panic if the "handshake state must exist"
/// invariant is broken.
pub async fn received_hello_packet_as_initializer(
    packet: HelloPacket,
    src_addr: SocketAddr,
    outbound_sender: ManagerToProcessor,
) {
    if get_state!()
        .session_exists(packet.proposed_session_id)
        .await
    {
        session_id_collided(packet, src_addr, outbound_sender).await;
        return;
    }

    let (sender, receiver) = mpsc::channel::<ConnectionEvent>(256);
    let (secret, response) = o_unwrap_or_return!(
        get_state!()
            .promote_handshake(
                packet.proposed_session_id,
                src_addr,
                packet.handshake_id,
                sender,
                packet.app_id,
            )
            .await
    );
    let key = key_exchange::get_shared_secret(secret, packet.public_key);
    let cipher = Aes256GcmSiv::new(&key.into());

    // insert cipher to state for any future encryption
    lock_write!(get_state!().encryption)
        .insert(packet.proposed_session_id, EncryptionWindow::new(cipher));

    _ = response.send(Ok((packet.proposed_session_id, receiver)));

    // construct ack and send
    Box::new(HandshakeAckPacket::new(
        packet.proposed_session_id,
        packet.handshake_id,
    ))
    .send(outbound_sender, src_addr)
    .await;
}

async fn session_id_collided(
    packet: HelloPacket,
    src_addr: SocketAddr,
    outbound_sender: ManagerToProcessor,
) {
    Box::new(HandshakeRejection::new(
        Options::none(),
        packet.proposed_session_id,
        HandshakeRejectionReason::IdCollision,
        packet.handshake_id,
        PayloadField::empty(),
    ))
    .send(outbound_sender.clone(), src_addr)
    .await;

    let (ephemeral_secret, public_key) = key_exchange::create();
    let handshake_id = o_unwrap_or_return!(
        get_state!()
            .reuse_handshake(
                packet.proposed_session_id,
                packet.handshake_id,
                ephemeral_secret,
            )
            .await
            .panic_in_debug(
                "Invariant broken in the `session_id_collided` routine: \
                the handshake did not exist"
            )
    );

    Box::new(HelloPacket::new(
        Options::none(),
        SessionId::generate(),
        handshake_id,
        public_key,
        get_state!().app_id(),
        get_state!().port(),
    ))
    .send(outbound_sender, src_addr)
    .await;
}

pub async fn received_handshake_ack_packet(packet: Box<HandshakeAckPacket>) {
    get_state!().handshake_done(packet.session_id).await;
}

pub async fn received_packet_that_requires_ack(
    session_id: SessionId,
    fingerprint: impl Into<PacketFingerprint>,
    sender: ManagerToProcessor,
) {
    let address = *lock_read!(match_or_return!(
        o_unwrap_or_return!(lock_read!(get_state!().connections).get(&session_id)),
        ConnectionStates::Established (box EstablishedState{ address, .. }) => address
    ));

    let fingerprint = fingerprint.into();

    AckPacket::new(Options::none(), session_id, fingerprint)
        .send(sender, address)
        .await;
}

pub async fn received_packet_with_invalid_session(
    src_addr: SocketAddr,
    sender: ManagerToProcessor,
    session_id: SessionId,
) {
    let packet = Box::new(SessionDoesNotExistErrorPacket::new(
        Options::none(),
        session_id,
    ));

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

/// Routine to handle an incompatible version error, occuring during deserialization.
/// The function sends an `IncompatibleVersionPacket` to the source, doing a reasonable effort to
/// make sure the packet arrives by sending it to the default port as well.
pub async fn received_packet_with_incompatible_version(
    src_addr: SocketAddr,
    sender: ManagerToProcessor,
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
