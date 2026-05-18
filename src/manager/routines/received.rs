use std::{net::SocketAddr, time::Duration};

use crate::{
    error::ConnectionError,
    lock,
    manager::state::{
        ConnectionStates, EstablishedState, SessionStates, StreamState, Streaming, StreamingTo,
    },
    packet_processor::fec,
    prelude::*,
};
use aes_gcm_siv::{Aes256GcmSiv, KeyInit};
use tokio::sync::mpsc::{self, Receiver};
use tracing::{debug, instrument, warn};

use crate::{
    DEFAULT_PORT, get_state, lock_read, lock_write,
    manager::{
        STATE, key_exchange,
        packets::*,
        state::EncryptionWindow,
        types::{ManagerToApi, ManagerToProcessor},
    },
    o_unwrap_or_return, r_unwrap_or_return,
    utils::{ApiMessage, AppResponse, Flags, InnerConnectionEvent, OneShot, SendPacket},
};

const BUFFERING_TIME_FOR_HANDSHAKE: u64 = 7;

pub async fn received_close_session_packet(packet: Box<CloseSessionPacket>) {
    notify_and_close_session(packet.session_id).await;
}

/// Notify any active session at `session_id` that it's been closed by the
/// peer (Streaming → flag closed; Established → `ConnectionClosed` event;
/// Handshake → nothing actionable on our side, the handshake will time out
/// naturally) and drop the local state.
async fn notify_and_close_session(session_id: SessionId) {
    {
        let lock = lock_read!(get_state!().connections);
        let session = o_unwrap_or_return!(lock.get(&session_id));
        if let ConnectionStates::Established(establised) = session {
            match establised.as_ref() {
                EstablishedState {
                    state: SessionStates::Streaming(StreamState { stream, .. }),
                    ..
                } => stream.send_modify(|m| m.closed = true),
                EstablishedState { connection, .. } => {
                    _ = connection
                        .send(InnerConnectionEvent::ConnectionClosed)
                        .await;
                }
            }
        }
    }

    get_state!().close_session(session_id).await;
}

/// Peer told us the `session_id` we used doesn't exist on their side. From
/// our perspective there's nothing to recover — the peer won't process any
/// more session-bound traffic from us. Treat it the same as a peer-initiated
/// close.
#[instrument(skip_all)]
pub async fn received_session_does_not_exist_error(packet: Box<SessionDoesNotExistErrorPacket>) {
    warn!(
        "peer reports session {} does not exist on their side; closing locally",
        packet.session_id
    );
    notify_and_close_session(packet.session_id).await;
}

/// Peer told us we sent an unexpected packet. This is a diagnostic — peer's
/// session is still alive and we shouldn't tear ours down (would drop
/// in-flight chunks / pending ops). Log enough to diagnose which packet
/// upset them; let the session continue.
#[instrument(skip_all)]
pub async fn received_unexpected_packet_error(packet: Box<UnexpectedPacketErrorPacket>) {
    warn!(
        "peer reports unexpected packet for session {} (received_packet_type: {:?}, \
         received_secondary_type: {:?}, fingerprint: {:?})",
        packet.session_id,
        packet.received_packet_type,
        packet.received_secondary_type,
        packet.received_fingerprint,
    );
}

#[instrument(skip_all)]
pub async fn received_track_reject_packet(packet: Box<TrackRejectionPacket>) {
    warn!(
        "peer's app rejected track request on session {}. track ID: {:?}",
        packet.session_id, packet.payload,
    );

    let lock = lock_read!(get_state!().connections);
    if let Some(ConnectionStates::Established(established)) = lock.get(&packet.session_id)
        && let EstablishedState {
            state: SessionStates::Streaming(StreamState { stream, .. }),
            ..
        } = established.as_ref()
    {
        stream.send_modify(|m| _ = m.approved.replace(false));
    }
}

/// Peer told us we're on an incompatible protocol version. The packet
/// carries no `session_id` (it can be sent before any session exists), so
/// there's nothing local to clean up here — any in-flight handshake to
/// this peer will time out naturally. Surfacing this to the API caller of
/// the affected handshake would need a handshake-by-addr lookup; deferred.
#[instrument(skip_all)]
pub async fn received_incompatible_version_error(
    packet: Box<IncompatibleVersionPacket>,
    src_addr: SocketAddr,
) {
    warn!(
        "peer at {src_addr} reports incompatible version (peer min version: {})",
        packet.min_version
    );
}

pub async fn received_retransmit_request(
    packet: Box<RetransmitPacket>,
    outbound_sender: ManagerToProcessor,
) {
    debug!(
        "received retransmit request for session {} ({} ranges)",
        packet.session_id,
        packet.payload.len()
    );

    // Auto-ack mirrors the data-packet path. Without this, every
    // RetransmitPacket the receiver sends sits unacked in its
    // PendingAckWindow and gets retried up to MAX_RETRIES times,
    // amplifying the outbound rate every TTL window.
    if packet.opts.contains(OptionFlags::RequireAck) {
        received_packet_that_requires_ack(packet.session_id, packet.as_ref(), outbound_sender)
            .await;
    }

    if let Some(ConnectionStates::Established(established)) =
        lock_read!(get_state!().connections).get(&packet.session_id)
        && let EstablishedState {
            state:
                SessionStates::Streaming(StreamState {
                    streaming:
                        Streaming::To(StreamingTo {
                            buffer: _,
                            current_batch: _,
                            event,
                            ..
                        }),
                    ..
                }),
            ..
        } = established.as_ref()
    {
        // Site 4: one served observation per inbound request. `payload.len()`
        // is the number of byte ranges the peer asked us to retransmit;
        // ranges get split into chunks downstream so the actual re-emit
        // count is captured by the regular `DataPacketSent` at site 1.
        if let Ok(served) = u32::try_from(packet.payload.len())
            && served > 0
        {
            get_state!()
                .data_collection
                .post(Observation::RetransmitServed {
                    session: packet.session_id,
                    count: served,
                });
        }
        event.update(StreamEvent::Retransmit(packet.payload)).await;
    }
}

pub async fn received_keep_alive_packet(packet: Box<KeepAlivePacket>) {
    get_state!()
        .global_handle_monitor
        .dispatch(update_last_activity(packet.session_id, packet.timestamp));
    get_state!()
        .update_address(packet.session_id, o_unwrap_or_return!(packet.address))
        .await;
}

pub async fn received_track_request_packet(packet: Box<TrackRequestPacket>) {
    get_state!()
        .global_handle_monitor
        .dispatch(update_last_activity(packet.session_id, packet.timestamp));

    let session_id = packet.session_id;
    let raw = packet.payload.take();

    // Payload layout (see `endpoints::request_track`):
    //   [scheme:u8][recovery_count:u8][batch_size:u8][track_id..]
    // A malformed/short payload is treated as a protocol error and dropped.
    if raw.len() < 3 {
        warn!(
            "TrackRequestPacket on session {session_id} has truncated payload ({}B), dropping",
            raw.len()
        );
        return;
    }
    let scheme = match raw[0] {
        x if x == FecScheme::Xor as u8 => FecScheme::Xor,
        x if x == FecScheme::ReedSolomon as u8 => FecScheme::ReedSolomon,
        other => {
            warn!("TrackRequestPacket on session {session_id} has unknown FecScheme {other}");
            return;
        }
    };
    let fec_config = FecConfig {
        scheme,
        recovery_count: raw[1],
        batch_size: raw[2],
    };
    let track_id: Box<[u8]> = raw[3..].to_vec().into();

    if let Some(ConnectionStates::Established(established)) =
        lock_read!(get_state!().connections).get(&session_id)
        && let EstablishedState { connection, .. } = established.as_ref()
    {
        _ = connection
            .send(InnerConnectionEvent::TrackRequest(track_id, fec_config))
            .await;
    }
}

pub async fn update_last_activity(session_id: SessionId, ts: Timestamp) {
    if let Some(ConnectionStates::Established(established)) =
        lock_read!(get_state!().connections).get(&session_id)
        && let EstablishedState { last_activity, .. } = established.as_ref()
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
                _ = handshake
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
                _ = ack_triggered_response.send(Err(ConnectionError::SessionIdCollided));
            }
        }
    }
}

pub async fn received_ack_packet(packet: Box<AckPacket>) {
    update_last_activity(packet.session_id, packet.timestamp).await;
    get_state!().ack.acknowledge(packet.fingerprint).await;
}

pub async fn received_parity_packet(packet: ParityPacket, outbound_sender: ManagerToProcessor) {
    get_state!()
        .global_handle_monitor
        .dispatch(update_last_activity(packet.session_id, packet.timestamp));

    let session_id = packet.session_id;

    let batch_id = packet.batch_id;
    let scheme = packet.fec_info.scheme;
    let fingerprint = PacketFingerprint::from(&packet);

    if fec::received(packet).await {
        let recovered_result = {
            let mut lock = lock_write!(get_state!().connections);
            let recovered = o_unwrap_or_return!(fec::recover(batch_id, session_id, scheme).await);
            o_unwrap_or_return!(lock.get_mut(&session_id))
                .recovered_packet(recovered, outbound_sender.clone())
                .await
        };

        match recovered_result {
            Err(Error::FailedToDeref) => failed_to_deref_buffer(session_id).await,
            Err(Error::StateMismatch { .. }) => {
                UnexpectedPacketErrorPacket::unexpected(
                    Options::none(),
                    session_id,
                    PacketType::Data,
                    SecondaryType::none(),
                    fingerprint,
                )
                .send(
                    outbound_sender,
                    o_unwrap_or_return!(lock_read!(get_state!().connections).get(&session_id))
                        .address()
                        .await,
                )
                .await;
            }
            Ok(()) | Err(_) => {}
        }
    }
}

pub async fn received_data_packet(packet: DataPacket, outbound_sender: ManagerToProcessor) {
    get_state!()
        .global_handle_monitor
        .dispatch(update_last_activity(packet.session_id, packet.timestamp));

    if let ConnectionStates::Handshake { signal, .. } =
        o_unwrap_or_return!(lock_read!(get_state!().connections).get(&packet.session_id))
    {
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
    let batch_id = packet.batch_id;
    let scheme = packet.fec_info.scheme;

    let result = {
        let mut guard = lock_write!(get_state!().connections);
        let Some(state) = guard.get_mut(&packet.session_id) else {
            return;
        };
        state
            .received_data_packet(packet, outbound_sender.clone())
            .await
    };

    match result {
        Ok(b) if b => {
            let recovered = o_unwrap_or_return!(fec::recover(batch_id, session_id, scheme).await);
            let mut lock = lock_write!(get_state!().connections);
            if let Err(Error::FailedToDeref) = o_unwrap_or_return!(lock.get_mut(&session_id))
                .recovered_packet(recovered, outbound_sender)
                .await
            {
                drop(lock);
                failed_to_deref_buffer(session_id).await;
            }
        }
        Err(Error::StateMismatch { .. }) => {
            UnexpectedPacketErrorPacket::unexpected(
                Options::none(),
                session_id,
                PacketType::Data,
                SecondaryType::none(),
                fingerprint,
            )
            .send(
                outbound_sender,
                o_unwrap_or_return!(lock_read!(get_state!().connections).get(&session_id))
                    .address()
                    .await,
            )
            .await;
        }
        Err(Error::FailedToDeref) => {
            failed_to_deref_buffer(session_id).await;
        }
        Err(e) => {
            warn!("received_data_packet: unexpected error for session {session_id}: {e:?}");
        }
        _ => {}
    }
}

async fn failed_to_deref_buffer(session_id: SessionId) {
    {
        let lock = lock_write!(get_state!().connections);
        let session = o_unwrap_or_return!(lock.get(&session_id));

        if let ConnectionStates::Established(established) = session
            && let EstablishedState {
                state: SessionStates::Streaming(StreamState { stream, .. }),
                ..
            } = established.as_ref()
        {
            stream.send_modify(|m| m.buffer_closed = true);
        }
    }

    // TODO: in the future just close stream
    get_state!().close_session(session_id).await;
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
    src_addr: SocketAddr,
    outbound_sender: ManagerToProcessor,
    app_sender: ManagerToApi,
) {
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
        core::result::Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>,
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

    let (sender, receiver) = mpsc::channel::<InnerConnectionEvent>(256);
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

pub async fn received_playback_control_packet(
    packet: Box<PlaybackControlPacket>,
    outbound_sender: ManagerToProcessor,
) {
    match packet.control_type {
        _new_event @ (PlaybackControlType::Done
        | PlaybackControlType::Pause
        | PlaybackControlType::Play
        | PlaybackControlType::Seek) => {
            if let Some(ConnectionStates::Established(established)) =
                lock_read!(get_state!().connections).get(&packet.session_id)
                && let EstablishedState {
                    state:
                        SessionStates::Streaming(StreamState {
                            streaming: Streaming::To(StreamingTo { event, .. }),
                            ..
                        }),
                    ..
                } = established.as_ref()
            {
                event.update((*packet).into()).await;
            }
        }
        PlaybackControlType::Close => {
            get_state!()
                .close_stream(packet.session_id, outbound_sender)
                .await;
        }
    }
}

pub async fn received_handshake_ack_packet(packet: Box<HandshakeAckPacket>) {
    get_state!().handshake_done(packet.session_id).await;
}

pub async fn received_packet_that_requires_ack(
    session_id: SessionId,
    fingerprint: impl Into<PacketFingerprint>,
    sender: ManagerToProcessor,
) {
    let address = {
        if let Some(ConnectionStates::Established(established)) =
            lock_read!(get_state!().connections).get(&session_id)
            && let EstablishedState { address, .. } = established.as_ref()
        {
            *lock_read!(address)
        } else {
            return;
        }
    };

    let fingerprint = fingerprint.into();

    AckPacket::new(Options::none(), session_id, fingerprint)
        .send(sender, address)
        .await;
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
