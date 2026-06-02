#![allow(clippy::wildcard_imports)]
use std::{
    net::SocketAddr,
    ops::Range,
    sync::mpsc::{SyncSender, sync_channel},
    thread::JoinHandle,
    time::Duration,
};

use tokio::{
    runtime::Builder as RuntimeBuilder,
    sync::{mpsc::Receiver, oneshot},
};
use tracing::info;
use tracing::{debug, instrument, warn};

use crate::{
    error::{ApiErrors, ConnectionError, EmptyResult},
    get_state, lock_read, lock_write,
    manager::{
        self, AppId, STATE, key_exchange,
        packets::*,
        state::{
            ConnectionStates, EstablishedState, HandshakeId, SessionStates, StreamState, Streaming,
        },
        types::{ManagerFromApi, ManagerToApi, ManagerToProcessor},
    },
    o_unwrap_or_return,
    utils::{
        Flags, InnerConnectionEvent, LogFail, OneShot, PanicInDebug, PlaybackControl,
        RequestDataRequest, SendDataRequest, SendPacket, SendTarget, not,
    },
};

const CLOSE_SESSION_DELAY: Duration = Duration::from_millis(25);

/// Creates a runtime and initiates the protocol in it.
///
/// # Errors
/// will return an error if creating the runtime fails.
///
/// # Panics
/// This function might panic if the oneshot channel used for the protocol to communicate an error
/// closed before sending.
#[instrument(skip_all)]
pub fn open(
    port: u16,
    app_id: AppId,
    manager_to_api: ManagerToApi,
    manager_from_api: ManagerFromApi,
) -> core::result::Result<JoinHandle<core::result::Result<(), ApiErrors>>, ApiErrors> {
    info!("protocol opened -- port {}, id {}", port, app_id);

    let (mos, mor): (SyncSender<core::result::Result<(), ApiErrors>>, _) = sync_channel(1);

    let manager_handle = std::thread::spawn(move || {
        let thread_name = "Manager";
        let runtime = RuntimeBuilder::new_multi_thread()
            .enable_all()
            .thread_name(thread_name)
            .build()
            .map_err(ApiErrors::FailedToBuildRuntime)
            .log_error("Failed to build runtime!");

        match runtime {
            Err(e) => {
                _ = mos.send(Err(e));
                Err(ApiErrors::ThreadFailed(thread_name))
                    .log_error(&format!("failed to build thread {thread_name}"))
            }
            Ok(runtime) => {
                _ = mos.send(Ok(()));
                runtime.block_on(async {
                    manager::init(port, app_id, manager_to_api, manager_from_api).await
                })
            }
        }
    });

    mor.recv().expect(
        "Invariant broken while receiving on sync oneshot channel for manager:\
                            Channel closed before sending",
    )?;

    Ok(manager_handle)
}

/// Root Routine for the [`API::connect`] endpoint.
/// This function creates a handshake entry and sends a [`HelloPacket`] to the given address,
/// assuming that is the receiving port for the host.
#[instrument(skip_all)]
pub async fn connect(
    OneShot {
        data: address,
        response,
    }: OneShot<
        SocketAddr,
        core::result::Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>,
    >,
    outbound_sender: ManagerToProcessor,
) {
    info!("connection requested with {:?}", address);

    let session_id = SessionId::generate();
    let (ephemeral_secret, public_key) = key_exchange::create();
    let handshake_id = HandshakeId::generate().await;

    get_state!()
        .handshakes
        .new_handshake(
            handshake_id,
            address,
            ephemeral_secret,
            session_id,
            response,
        )
        .await;

    Box::new(HelloPacket::new(
        Options::none(),
        session_id,
        handshake_id,
        public_key,
        get_state!().app_id(),
    ))
    .send(outbound_sender, address)
    .await;
}

/// Root Routine for the [`Api::send_data`] endpoint.
/// Resolves the target to a session, validates it is free, and sends a single [`DataPacket`].
/// Replies to the caller via the embedded oneshot with `Ok(())` on success or an [`ApiErrors`]
/// variant on failure.
#[instrument(skip_all)]
pub async fn send_data(
    OneShot {
        data:
            SendDataRequest {
                target,
                buffer,
                sender,
            },
        response: reply,
    }: OneShot<SendDataRequest, core::result::Result<SessionId, ApiErrors>>,
    outbound_sender: ManagerToProcessor,
) {
    info!("trying to send data to {:?}", target);

    let session_id = match resolve_target(&target).await {
        Ok((session_id, _)) => session_id,
        Err(e) => {
            _ = reply.send(Err(e).panic_in_debug("Failed while resolving target!"));
            return;
        }
    };
    _ = reply.send(Ok(session_id));

    get_state!()
        .send_on_session(session_id, buffer, sender, outbound_sender)
        .await;
}

#[instrument(skip_all)]
async fn resolve_target(
    target: &SendTarget,
) -> core::result::Result<(SessionId, SocketAddr), ApiErrors> {
    info!("trying to resolve target {target:?}");
    match target {
        SendTarget::Session(session_id) => {
            if let ConnectionStates::Established(established) = lock_read!(get_state!().connections)
                .get(session_id)
                .ok_or(ApiErrors::SessionDoesNotExist)?
                && let EstablishedState {
                    address,
                    state: SessionStates::Up | SessionStates::Down,
                    ..
                } = established.as_ref()
            {
                let address = *lock_read!(address);

                Ok((*session_id, address))
            } else {
                Err(ApiErrors::SessionOccupied)
            }
        }
        SendTarget::Address(address) => get_state!()
            .address_session
            .free_session(*address)
            .await
            .ok_or(ApiErrors::NoFreeSession)
            .map(|s| (s, *address)),
    }
    .log_warn(&format!("could not resolve target {target:?}"))
}

#[instrument(skip_all)]
pub async fn request_track(
    OneShot {
        data:
            RequestDataRequest {
                target,
                id,
                buffer,
                sender,
            },
        response,
    }: OneShot<RequestDataRequest, Result<SessionId, ApiErrors>>,
    outbound_sender: ManagerToProcessor,
) {
    info!("requesting track from {:?}, id: \"{:?}\"", target, id);

    debug_assert!(
        id.len() <= MAX_PAYLOAD_LENGTH,
        "Invariant broken in `request_track`: id exceeds `MAX_PAYLOAD_LENGTH` ({} > {})",
        id.len(),
        MAX_PAYLOAD_LENGTH,
    );

    let (session_id, addr) = match resolve_target(&target).await {
        Ok(pair) => pair,
        Err(ApiErrors::SessionOccupied) => {
            // HACK: give some grace time for a stream that might be currently closing
            tokio::time::sleep(Duration::from_millis(100)).await;
            match resolve_target(&target).await {
                Ok(pair) => pair,
                Err(e) => {
                    _ = response.send(Err(e));
                    return;
                }
            }
        }
        Err(e) => {
            _ = response.send(Err(e));
            return;
        }
    };

    {
        let mut lock = lock_write!(get_state!().connections);
        let Some(state) = lock.get_mut(&session_id).panic_in_debug(&format!(
            "Invariant broken in `request_track`: session {session_id} does not exist"
        )) else {
            _ = response.send(Err(ApiErrors::SessionDoesNotExist));
            return;
        };
        state.streaming_from(buffer, sender);
    }

    _ = response.send(Ok(session_id));

    debug!("session {} switched to `StreamingFrom` state", session_id);

    TrackRequestPacket::request_track(Options::none(), session_id, id.into())
        .send(outbound_sender, addr)
        .await;
}

//pub fn request_metadata() {
//    todo!()
//}

#[instrument(skip_all)]
pub async fn close_session(session_id: SessionId, sender: ManagerToProcessor) {
    info!("closing session {}", session_id);

    // a delay is added as a best effort to add a small buffer for other packets to be received by the other host,
    // since ordering isnt guaranteed and receiving this packet will cause the other host to
    // immediately stop accepting from this session
    tokio::time::sleep(CLOSE_SESSION_DELAY).await;

    Box::new(CloseSessionPacket::new(Options::none(), session_id))
        .send(
            sender,
            o_unwrap_or_return!(lock_read!(get_state!().connections).get(&session_id))
                .address()
                .await,
        )
        .await;

    debug!("session {} close packet sent.", session_id);
    get_state!().close_session(session_id).await;
}

#[instrument(skip_all)]
pub async fn close_stream(session_id: SessionId, sender: ManagerToProcessor) {
    info!("closing stream for session {}", session_id);

    get_state!().close_stream(session_id, sender.clone()).await;

    let address = o_unwrap_or_return!(lock_read!(get_state!().connections).get(&session_id))
        .address()
        .await;

    Box::new(PlaybackControlPacket::close(Options::none(), session_id))
        .send(sender, address)
        .await;
}

pub async fn set_complete_stream(session_id: SessionId, allow_partial: bool) {
    if let Some(ConnectionStates::Established(established)) =
        lock_read!(get_state!().connections).get(&session_id)
        && let EstablishedState {
            state: SessionStates::Streaming(StreamState { stream, .. }),
            ..
        } = established.as_ref()
    {
        stream.send_modify(|m| _ = m.complete.replace(allow_partial));
    }
}

#[instrument(skip_all)]
pub async fn send_playback_control_packet(
    session_id: SessionId,
    control: PlaybackControl,
    response: oneshot::Sender<EmptyResult>,
    sender: ManagerToProcessor,
) {
    let address = if let Some(session) = lock_read!(get_state!().connections).get(&session_id)
        && let ConnectionStates::Established(established) = session
        && let EstablishedState { address, .. } = established.as_ref()
    {
        *lock_read!(address)
    } else {
        debug!("playback action taken on a nonexistant session");
        _ = response.send(Err(()));
        return;
    };

    let changed = match control {
        PlaybackControl::Play => update_paused(session_id, false).await,
        PlaybackControl::Pause => update_paused(session_id, true).await,
        _ => None,
    };

    if changed.is_some_and(not) {
        _ = response.send(Ok(()));
        return;
    }

    let (control, seek_pos) = match control {
        PlaybackControl::Play => (PlaybackControlType::Play, None),
        PlaybackControl::Pause => (PlaybackControlType::Pause, None),
        PlaybackControl::Close => (PlaybackControlType::Close, None),
        PlaybackControl::Done => (PlaybackControlType::Done, None),
        PlaybackControl::Seek(byte_position) => {
            if let Some(moved_forward) = seek(session_id, byte_position).await
                && moved_forward
            {
                (PlaybackControlType::Seek, Some(byte_position))
            } else {
                _ = response.send(Ok(()));
                return;
            }
        }
    };

    #[cfg(debug_assertions)]
    match control {
        PlaybackControlType::Play
        | PlaybackControlType::Pause
        | PlaybackControlType::Close
        | PlaybackControlType::Done => {
            debug!("sending playback control {control} on session {session_id}");
        }
        PlaybackControlType::Seek => {
            debug!("sending seek control packet to {seek_pos:?} on session {session_id}");
        }
    }

    _ = response.send(Ok(()));

    Box::new(PlaybackControlPacket::new(
        Options::construct(&[OptionFlags::RequireAck]),
        session_id,
        control,
        seek_pos,
    ))
    .send(sender, address)
    .await;
}

async fn update_paused(session_id: SessionId, paused: bool) -> Option<bool> {
    if let Some(ConnectionStates::Established(established)) =
        lock_read!(get_state!().connections).get(&session_id)
        && let EstablishedState {
            state: SessionStates::Streaming(StreamState { stream, .. }),
            ..
        } = established.as_ref()
    {
        if paused == stream.borrow().paused {
            return Some(false);
        }

        let _current = stream.borrow().paused;
        stream.send_modify(|m| m.paused = paused);
        Some(true)
    } else {
        None
    }
}

async fn seek(session_id: SessionId, pos: BytePosition) -> Option<bool> {
    let mut lock = lock_write!(get_state!().connections);
    if let Some(ConnectionStates::Established(established)) = lock.get_mut(&session_id)
        && let EstablishedState {
            state:
                SessionStates::Streaming(StreamState {
                    streaming: Streaming::From(streaming_from),
                    ..
                }),
            ..
        } = established.as_mut()
    {
        streaming_from.buffer.seek_head(pos)
    } else {
        None
    }
}

pub async fn find_holes(
    session_id: SessionId,
    response: oneshot::Sender<Option<Vec<Range<usize>>>>,
) {
    let to_send = {
        let lock = lock_read!(get_state!().connections);
        if let Some(ConnectionStates::Established(established)) = lock.get(&session_id)
            && let EstablishedState {
                state:
                    SessionStates::Streaming(StreamState {
                        streaming: Streaming::From(streaming_from),
                        ..
                    }),
                ..
            } = established.as_ref()
        {
            let buf = &streaming_from.buffer;
            Some(
                buf.find_holes(BytePosition::from(buf.len()))
                    .into_iter()
                    .map(|pos| Range {
                        start: *pos as usize,
                        end: (*pos as usize) + MAX_PAYLOAD_LENGTH,
                    })
                    .collect(),
            )
        } else {
            None
        }
    };

    _ = response.send(to_send);
}

pub async fn reject_track_request(
    session_id: SessionId,
    track_id: Box<[u8]>,
    sender: ManagerToProcessor,
) {
    let address = {
        if let Some(ConnectionStates::Established(established)) =
            lock_read!(get_state!().connections).get(&session_id)
            && let EstablishedState {
                state: SessionStates::Up | SessionStates::Down,
                address,
                ..
            } = established.as_ref()
        {
            *lock_read!(address)
        } else {
            return;
        }
    };

    Box::new(TrackRejectionPacket::new(
        Options::construct(&[OptionFlags::RequireAck]),
        session_id,
        track_id,
    ))
    .send(sender, address)
    .await;
}
