use std::default;
use std::{fmt::Display, net::SocketAddr};

use crate::api::ReadableBuffer;
use crate::error::ApiErrors;

use derive_more::{Display, derive};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;

use crate::manager::AppId;
use crate::packet_processor::fec::Recovered;
use crate::prelude::*;
use crate::{
    manager::packets::{BatchID, PacketWrapper, PayloadField, SessionId},
    packet_processor::{fec::RecoverdPacket, types::ProcessedPacket},
    transport::types::ReceivedPacket,
};

#[derive(Debug)]
pub struct ResponseReceiver<T>(oneshot::Receiver<T>);

impl<T> ResponseReceiver<T> {
    pub async fn recv(self) -> core::result::Result<T, ApiErrors> {
        match self.0.await {
            Err(_) => Err(ApiErrors::ProtocolClosed),
            Ok(response) => Ok(response),
        }
    }
}

#[derive(Debug)]
pub struct OneShot<T: Send, Res: Send> {
    pub data: T,
    pub response: oneshot::Sender<Res>,
}

impl<Req: Send, Res: Send> OneShot<Req, Res> {
    pub fn new(value: Req) -> (Self, ResponseReceiver<Res>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                data: value,
                response: sender,
            },
            ResponseReceiver(receiver),
        )
    }
}

impl<Req: Send> OneShot<Req, AppResponse> {
    pub fn app(value: Req) -> (Self, ResponseReceiver<AppResponse>) {
        OneShot::new(value)
    }
}

impl<Req: Send> OneShot<Req, core::result::Result<Recovered, CouldNotRecover>> {
    pub fn processor(
        value: Req,
    ) -> (
        Self,
        ResponseReceiver<core::result::Result<Recovered, CouldNotRecover>>,
    ) {
        OneShot::new(value)
    }
}

#[derive(Debug)]
pub enum AppResponse {
    AppApproved,
    AppRejected(String),
}

pub enum ApiMessage {
    IncommingConncetion {
        request: OneShot<AppId, AppResponse>,
        response: ResponseReceiver<
            core::result::Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>,
        >,
        peer_address: SocketAddr,
    },
    DataReceived {
        session_id: SessionId,
        data: PayloadField,
    },
}

#[derive(Debug)]
pub enum ConnectionEvent {
    DataReceived(PayloadField),
    TrackRequest(Box<[u8]>),
    Closed(Vec<ConnectionEvent>),
}

#[derive(Debug, Default)]
pub struct StreamMessage {
    pub head: usize,
    pub is_paused: bool,
    pub closed: bool,
}

pub enum SendTarget {
    Address(SocketAddr),
    Session(SessionId),
}

pub struct RequestDataRequest {
    pub target: SendTarget,
    pub id: Box<[u8]>,
}

pub struct SendDataRequest {
    pub target: SendTarget,
    pub buffer: ReadableBuffer,
}

pub enum ApiCommand {
    Connect(
        OneShot<
            SocketAddr,
            core::result::Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>,
        >,
    ),
    RequestData(OneShot<RequestDataRequest, core::result::Result<SessionId, ApiErrors>>),
    SendData(OneShot<SendDataRequest, core::result::Result<SessionId, ApiErrors>>),
    Close,
}

impl<Req: Send> OneShot<Req, core::result::Result<(), ApiErrors>> {
    pub fn command(value: Req) -> (Self, ResponseReceiver<core::result::Result<(), ApiErrors>>) {
        OneShot::new(value)
    }
}

pub enum ManagerMessage {
    Recovered(Recovered),
    Packet(PacketWrapper),
    Closed,
}

#[derive(thiserror::Error, Debug, Display)]
pub struct CouldNotRecover;

#[derive(Debug)]
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
