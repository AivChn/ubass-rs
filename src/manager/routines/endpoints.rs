#![allow(clippy::wildcard_imports)]
use std::{
    net::SocketAddr,
    sync::mpsc::{SyncSender, sync_channel},
    thread::JoinHandle,
};

use tokio::{runtime::Builder as RuntimeBuilder, sync::mpsc::Receiver};

use crate::{
    api::ApiErrors,
    error::ConnectionError,
    get_state, lock_read,
    manager::{
        self, AppId, STATE, key_exchange,
        packets::*,
        state::{
            ConnectionStates, HandshakeId, SessionStateFlag, SessionStateFlags, SessionStates,
        },
        types::{ManagerFromApi, ManagerToApi, ManagerToProcessor},
    },
    utils::{
        ConnectionEvent, Flags, OneShot, RequestDataRequest, SendDataRequest, SendPacket,
        SendTarget,
    },
};

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
                mos.send(Err(e));
                Err(ApiErrors::ThreadFailed(thread_name))
            }
            Ok(runtime) => {
                mos.send(Ok(()));
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
        data: SendDataRequest { target, buffer },
        response: reply,
    }: OneShot<SendDataRequest, core::result::Result<SessionId, ApiErrors>>,
    outbound_sender: ManagerToProcessor,
) {
    debug_assert!(
        buffer.len() <= MAX_PAYLOAD_LENGTH,
        "Invariant broken in `send_data`: buffer exceeds `MAX_PAYLOAD_LENGTH` ({} > {})",
        buffer.len(),
        MAX_PAYLOAD_LENGTH
    );

    let (session_id, addr) = match resolve_target(target).await {
        Ok(pair) => pair,
        Err(e) => {
            reply.send(Err(e));
            return;
        }
    };
    reply.send(Ok(session_id));

    DataPacket::new(
        Options::none(),
        BatchID::new(1),
        FECInfo::new(1, 0, 0),
        session_id,
        BytePosition(0),
        buffer.into_vec(),
    )
    .send(outbound_sender, addr)
    .await;
}

async fn resolve_target(
    target: SendTarget,
) -> core::result::Result<(SessionId, SocketAddr), ApiErrors> {
    match target {
        SendTarget::Session(session_id) => {
            let lock = lock_read!(get_state!().connection_state);
            let connection = lock
                .get(&session_id)
                .ok_or(ApiErrors::SessionDoesNotExist)?;

            let address = match connection {
                ConnectionStates::Established {
                    address,
                    state: SessionStates::Up | SessionStates::Down,
                    ..
                } => *lock_read!(address),
                _ => return Err(ApiErrors::SessionOccupied),
            };
            Ok((session_id, address))
        }
        SendTarget::Address(addr) => {
            let session_id = get_state!()
                .address_sessions
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
        data: RequestDataRequest { target, id },
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
            response.send(Err(e));
            return;
        }
    };
    response.send(Ok(session_id));

    TrackRequestPacket::request_track(Options::none(), session_id, id.into())
        .send(outbound_sender, addr)
        .await;
}

pub fn request_metadata() {
    todo!()
}
