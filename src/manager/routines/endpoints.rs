#![allow(clippy::wildcard_imports)]
use std::{
    net::SocketAddr,
    os::unix::fs::lchown,
    sync::mpsc::{SyncSender, sync_channel},
    thread::JoinHandle,
    time::Duration,
};

use tokio::{runtime::Builder as RuntimeBuilder, sync::mpsc::Receiver};

use crate::{
    api::WriteableBuffer,
    error::{ApiErrors, ConnectionError},
    get_state, lock_read, lock_write,
    manager::{
        self, AppId, STATE, key_exchange,
        packets::*,
        state::{
            ConnectionStates, EstablishedState, HandshakeId, SessionStateFlag, SessionStateFlags,
            SessionStates, StreamState, Streaming, StreamingTo,
        },
        types::{ManagerFromApi, ManagerToApi, ManagerToProcessor},
    },
    o_unwrap_or_return,
    utils::{
        ConnectionEvent, Flags, OneShot, PanicInDebug, RequestDataRequest, SendDataRequest,
        SendPacket, SendTarget, StreamEvent,
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
pub fn open(
    port: u16,
    app_id: AppId,
    manager_to_api: ManagerToApi,
    manager_from_api: ManagerFromApi,
) -> core::result::Result<JoinHandle<core::result::Result<(), ApiErrors>>, ApiErrors> {
    let (mos, mor): (SyncSender<core::result::Result<(), ApiErrors>>, _) = sync_channel(1);

    // ==================== manager =======================
    let manager_handle = std::thread::spawn(move || {
        let thread_name = "Manager";
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .thread_name(thread_name)
            .build()
            .map_err(ApiErrors::FailedToBuildRuntime);

        match runtime {
            Err(e) => {
                _ = mos.send(Err(e));
                Err(ApiErrors::ThreadFailed(thread_name))
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
pub async fn connect(
    OneShot {
        data: address,
        response,
    }: OneShot<
        SocketAddr,
        core::result::Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>,
    >,
    outbound_sender: ManagerToProcessor,
) {
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

    HelloPacket::new(
        Options::none(),
        session_id,
        handshake_id,
        public_key,
        get_state!().app_id(),
        get_state!().port(),
    )
    .send(outbound_sender, address)
    .await;
}

/// Root Routine for the [`Api::send_data`] endpoint.
/// Resolves the target to a session, validates it is free, and sends a single [`DataPacket`].
/// Replies to the caller via the embedded oneshot with `Ok(())` on success or an [`ApiErrors`]
/// variant on failure.
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
    let session_id = match resolve_target(target).await {
        Ok((session_id, _)) => session_id,
        Err(e) => {
            _ = reply.send(Err(e));
            return;
        }
    };
    _ = reply.send(Ok(session_id));

    get_state!()
        .send_on_session(session_id, buffer, sender, outbound_sender)
        .await;
}

async fn resolve_target(
    target: SendTarget,
) -> core::result::Result<(SessionId, SocketAddr), ApiErrors> {
    match target {
        SendTarget::Session(session_id) => {
            let lock = lock_read!(get_state!().connections);
            let connection = lock
                .get(&session_id)
                .ok_or(ApiErrors::SessionDoesNotExist)?;

            let address = match connection {
                ConnectionStates::Established(box EstablishedState {
                    address,
                    state: SessionStates::Up | SessionStates::Down,
                    ..
                }) => *lock_read!(address),
                _ => return Err(ApiErrors::SessionOccupied),
            };
            Ok((session_id, address))
        }
        SendTarget::Address(addr) => {
            let session_id = get_state!()
                .address_session
                .free_session(addr)
                .await
                .ok_or(ApiErrors::NoFreeSession)?;

            Ok((session_id, addr))
        }
    }
}

pub fn close() {
    todo!()
}

pub fn listen() {
    todo!()
}

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
    debug_assert!(
        id.len() <= MAX_PAYLOAD_LENGTH,
        "Invariant broken in `send_data`: buffer exceeds `MAX_PAYLOAD_LENGTH` ({} > {})",
        id.len(),
        MAX_PAYLOAD_LENGTH
    );

    let (session_id, addr) = match resolve_target(target).await {
        Ok(pair) => pair,
        Err(e) => {
            _ = response.send(Err(e));
            return;
        }
    };
    _ = response.send(Ok(session_id));

    o_unwrap_or_return!(
        lock_write!(get_state!().connections)
            .get_mut(&session_id)
            .panic_in_debug(&format!(
                "Invariant broken in `request_track`: session {session_id} does not exist"
            ))
    )
    .streaming_from(buffer, sender);

    TrackRequestPacket::request_track(Options::none(), session_id, id.into())
        .send(outbound_sender, addr)
        .await;
}

pub fn request_metadata() {
    todo!()
}

pub async fn close_session(session_id: SessionId, sender: ManagerToProcessor) {
    // a delay is added as a best effort to add a small buffer for other packets to be received by the other hoest,
    // since ordering isnt guaranteed and receiving this packet will cause the other host to
    // immediately stop accepting from this session
    tokio::time::sleep(CLOSE_SESSION_DELAY);

    Box::new(CloseSessionPacket::new(Options::none(), session_id))
        .send(
            sender,
            o_unwrap_or_return!(get_state!().connections.address(session_id).await),
        )
        .await;

    get_state!().close_session(session_id).await;
}

pub async fn close_stream(session_id: SessionId, sender: ManagerToProcessor) {
    get_state!().close_stream(session_id, sender.clone()).await;

    let address = o_unwrap_or_return!(get_state!().connections.address(session_id).await);

    Box::new(PlaybackStatusPacket::stop(Options::none(), session_id)).send(sender, address);
}
