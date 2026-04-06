mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
mod state;
pub mod types;

use std::{
    cell::OnceCell,
    net::SocketAddr,
    sync::{LazyLock, OnceLock},
};

use crate::{
    manager::{
        packets::{
            AppRejectErrorPacket, HelloPacket, IncompatibleVersionPacket, OptionFlags, Options,
            Packet, PacketFingerprint, PublicKey, SessionId, Version,
        },
        state::{EncryptionTable, EncryptionWindow, GeneralStateTable, Port, SessionStates},
    },
    prelude::*,
    read_lock, write_lock,
};

use aes_gcm_siv::{Aes256GcmSiv, KeyInit};
use tokio::{sync::RwLock, time::Instant};

pub use state::{AppId, EncryptionMonitor, FingerprintMonitor, PendingAckMonitor};
use types::*;

static STATE: OnceLock<SessionStates> = OnceLock::new();

macro_rules! get_state {
    () => {
        STATE.get().expect("State accessed before protocol open")
    };
}

pub fn init() {
    PROTOCOL_EPOCH.get_or_init(Instant::now);
}

pub fn open(port: Port) {
    STATE.set(SessionStates::new(port));
}

async fn initiate_handshake() -> Result<i32> {
    todo!()
}

async fn received_packet_with_incompatible_version(
    version: Version,
    src_addr: SocketAddr,
    sender: OutboundSender,
) {
    let packet = Packet::IncompatibleVersion(Box::new(IncompatibleVersionPacket::packet()));
    sender
        .send(PacketProcessingMessage::SendPacket(packet.wrap(src_addr)))
        .await;
}

async fn connect(address: SocketAddr, app_id: AppId) {
    let session_id = SessionId::new();
    let (ephemeral, public_key) = key_exchange::get();

    get_state!().new_session(session_id, address, app_id).await;
    todo!("save ephemeral part somewhere");

    let hello_packet = HelloPacket::new(
        Options::construct(&[]),
        todo!("session_id"),
        todo!("public_key"),
        app_id,
        get_state!().port().await,
    );
}

async fn received_hello_packet(
    packet: HelloPacket,
    mut src_addr: SocketAddr,
    outbound_sender: OutboundSender,
    app_sender: AppSender,
) {
    let (request, receiver) = OneShot::new(packet.app_id.clone());
    let request = AppRequest::HelloAppId(request);

    _ = app_sender.send(request).await;

    match receiver.await {
        Err(_) => {
            // TODO: log here
            return;
        }
        Ok(AppResponse::AppRejected(message)) => {
            let rejected_packet = Box::new(AppRejectErrorPacket::new(
                Options::construct(&[]),
                packet.proposed_session_id,
                packets::PacketType::Host,
                packets::ControlType::Host(packets::HostControlType::Hello).into(),
                PacketFingerprint::from(&packet),
                message,
            ));

            outbound_sender
                .send(PacketProcessingMessage::SendPacket(
                    Packet::AppRejectErrorPacket(rejected_packet)
                        .wrap(todo!("fill with actual address")),
                ))
                .await;
        }
        Ok(AppResponse::AppApproved(host_app_id)) => {
            let session_id = if get_state!()
                .session_exists(packet.proposed_session_id)
                .await
            {
                SessionId::new()
            } else {
                packet.proposed_session_id
            };

            src_addr.set_port(*packet.receiving_port);
            get_state!().new_session(session_id, src_addr, packet.app_id);

            let (key_part, public_key) = key_exchange::get();
            let key = key_exchange::get_shared_secret(key_part, (*packet.public_key).into());
            write_lock!(get_state!().encryption).insert(
                session_id,
                EncryptionWindow::new(Aes256GcmSiv::new((&key).into())),
            );

            let hello_packet = Box::new(HelloPacket::new(
                Options::construct(&[OptionFlags::RequireAck]),
                session_id,
                PublicKey::new(public_key),
                AppId::new(host_app_id),
                read_lock!(get_state!().port).clone(),
            ));

            outbound_sender.send(PacketProcessingMessage::SendPacket(
                Packet::HelloPacket(hello_packet).wrap(src_addr),
            ));
        }
    }
}
