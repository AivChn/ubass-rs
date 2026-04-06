use tokio::sync::oneshot;

use crate::manager::AppId;
use crate::prelude::*;
use crate::{
    manager::packets::{BatchID, PacketWrapper, SessionId},
    packet_processor::{fec::RecoverdPacket, types::ProcessedPacket},
    transport::types::ReceivedPacket,
};

pub struct OneShot<T: Send> {
    data: T,
    reply: oneshot::Sender<AppResponse>,
}

impl<T: Send> OneShot<T> {
    pub fn new(value: T) -> (Self, oneshot::Receiver<AppResponse>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                data: value,
                reply: sender,
            },
            receiver,
        )
    }
}

pub enum AppRequest {
    HelloAppId(OneShot<AppId>),
}

pub enum AppResponse {
    AppApproved(String),
    AppRejected(String),
}

pub enum ManagerMessage {
    Recovered(Vec<RecoverdPacket>),
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
