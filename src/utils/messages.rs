use std::fmt::Display;

use derive_more::Display;
use tokio::sync::oneshot;

use crate::manager::AppId;
use crate::packet_processor::fec::Recovered;
use crate::prelude::*;
use crate::{
    manager::packets::{BatchID, PacketWrapper, SessionId},
    packet_processor::{fec::RecoverdPacket, types::ProcessedPacket},
    transport::types::ReceivedPacket,
};

pub struct ResponseReceiver<T>(oneshot::Receiver<T>);

impl<T> ResponseReceiver<T> {
    pub async fn recv(self) -> T {
        match self.0.await {
            Err(_) => unreachable!(
                "Invariant broken while receiving on oneshot from app layer: \
                the sender was dropped before sending"
            ),
            Ok(response) => response,
        }
    }
}

pub struct OneShot<T: Send, Res: Send> {
    pub data: T,
    pub reply: oneshot::Sender<Res>,
}

impl<Req: Send> OneShot<Req, AppResponse> {
    pub fn app(value: Req) -> (Self, ResponseReceiver<AppResponse>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                data: value,
                reply: sender,
            },
            ResponseReceiver(receiver),
        )
    }
}

impl<Req: Send> OneShot<Req, core::result::Result<Recovered, CouldNotRecover>> {
    pub fn processor(
        value: Req,
    ) -> (
        Self,
        ResponseReceiver<core::result::Result<Recovered, CouldNotRecover>>,
    ) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                data: value,
                reply: sender,
            },
            ResponseReceiver(receiver),
        )
    }
}

pub enum AppMessage {
    HelloAppId(OneShot<AppId, AppResponse>),
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

#[derive(thiserror::Error, Debug, Display)]
pub struct CouldNotRecover;

pub enum PacketProcessingMessage {
    SendPacket(PacketWrapper),
    ReceivedPacket(ReceivedPacket),
    Recover(OneShot<(SessionId, BatchID), core::result::Result<Recovered, CouldNotRecover>>),
    Close,
    Closed,
}

pub enum TransportMessage {
    SendPacket(ProcessedPacket),
    Close,
}
