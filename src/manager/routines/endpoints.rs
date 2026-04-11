use std::{
    net::SocketAddr,
    sync::mpsc::{SyncSender, sync_channel},
    thread::JoinHandle,
};

use tokio::runtime::Builder as RuntimeBuilder;

use crate::{
    api::ApiErrors,
    get_state,
    manager::{self, AppId, STATE, key_exchange, packets::*, types::OutboundSender},
    utils::{Flags, SendPacket},
};

pub fn open(
    port: u16,
    app_id: AppId,
) -> core::result::Result<JoinHandle<core::result::Result<(), ApiErrors>>, ApiErrors> {
    let (mos, mor): (SyncSender<core::result::Result<(), ApiErrors>>, _) = sync_channel(1);

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
                runtime.block_on(async { manager::init(port, app_id).await })
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
pub async fn connect(address: SocketAddr, outbound_sender: OutboundSender) {
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

pub fn close() {
    todo!()
}

pub fn listen() {
    todo!()
}

pub fn request_track() {
    todo!()
}

pub fn request_metadata() {
    todo!()
}
