use tokio::sync::oneshot;

use crate::manager::AppId;
use crate::packet_processor::fec::Recovered;
use crate::prelude::*;
use crate::{
    manager::packets::{BatchID, PacketWrapper, SessionId},
    packet_processor::{fec::RecoverdPacket, types::ProcessedPacket},
    transport::types::ReceivedPacket,
};

pub struct AppResponseReceiver(oneshot::Receiver<AppResponse>);

impl AppResponseReceiver {
    pub async fn recv(self) -> AppResponse {
        match self.0.await {
            Err(_) => unreachable!(
                "Invariant broken while receiving on oneshot from app layer: \
                the sender was dropped before sending"
            ),
            Ok(response) => response,
        }
    }
}

pub struct OneShot<T: Send> {
    data: T,
    reply: oneshot::Sender<AppResponse>,
}

impl<T: Send> OneShot<T> {
    pub fn new(value: T) -> (Self, AppResponseReceiver) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                data: value,
                reply: sender,
            },
            AppResponseReceiver(receiver),
        )
    }
}

pub enum AppMessage {
    HelloAppId(OneShot<AppId>),
}

pub enum AppResponse {
    AppApproved,
    AppRejected(String),
}

pub enum ManagerMessage {
    Recovered(Recovered),
    Packet(PacketWrapper),
    Closed,
}

pub enum PacketProcessingMessage {
    SendPacket(PacketWrapper),
    ReceivedPacket(ReceivedPacket),
    Recover(SessionId, BatchID),
    Close,
    Closed,
}

pub enum TransportMessage {
    SendPacket(ProcessedPacket),
    Close,
}
