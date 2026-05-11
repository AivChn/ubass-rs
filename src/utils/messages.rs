use std::default;
use std::range::Range;
use std::{fmt::Display, net::SocketAddr};

use crate::api::{ReadableBuffer, WriteableBuffer};
use crate::error::ApiErrors;
use crate::manager::packets::{
    BytePosition, ByteRange, Options, PacketFingerprint, PlaybackControlPacket, PlaybackControlType,
};

use derive_more::{Display, derive};
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, watch};

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
    /// Receives from the channel, blocking until any result is available.
    ///
    /// # Errors
    /// This function will returned `ProtocolClosed` if the channel returns any error.
    /// This might happen even if the close was gracefull, and in some cases is expected on
    /// gracefull close.
    pub async fn recv(self) -> core::result::Result<T, ApiErrors> {
        self.0.await.map_err(|_| ApiErrors::ProtocolClosed)
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

#[derive(Debug)]
pub enum AppResponse {
    AppApproved,
    AppRejected(String),
}

pub enum ApiMessage {
    IncommingConncetion {
        request: OneShot<AppId, AppResponse>,
        response: ResponseReceiver<
            core::result::Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>,
        >,
        peer_address: SocketAddr,
    },
}

/// Manager-side message pushed onto a session's connection channel.
/// `Connection::listen()` translates these into the public `ConnectionEvent`
/// (in `api/core/types.rs`), wrapping primitives like `TrackRequest` into
/// `RequestedStream<Output>` so the app gets a typed accept/reject handle.
#[derive(Debug)]
pub enum InnerConnectionEvent {
    TrackRequest {
        track_id: Box<[u8]>,
        fingerprint: PacketFingerprint,
    },
    ProtocolClosed,
    ConnectionClosed,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StreamMessage {
    pub head: usize,
    pub paused: bool,
    pub closed: bool,
    pub complete: Option<bool>,
    pub approved: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum StreamEvent {
    Playback(PlaybackControl),
    Retransmit(Vec<ByteRange>),
}

impl Default for StreamEvent {
    fn default() -> Self {
        StreamEvent::Playback(PlaybackControl::Play)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum PlaybackControl {
    #[default]
    Play,
    Pause,
    Close,
    Done,
    Seek(BytePosition),
}

impl From<PlaybackControlPacket> for StreamEvent {
    fn from(value: PlaybackControlPacket) -> Self {
        StreamEvent::Playback(value.into())
    }
}

impl From<PlaybackControlPacket> for PlaybackControl {
    fn from(value: PlaybackControlPacket) -> Self {
        match value.control_type {
            PlaybackControlType::Play => PlaybackControl::Play,
            PlaybackControlType::Pause => PlaybackControl::Pause,
            PlaybackControlType::Close => PlaybackControl::Close,
            PlaybackControlType::Done => PlaybackControl::Done,
            PlaybackControlType::Seek => PlaybackControl::Seek(value.seek_position),
        }
    }
}

#[derive(Debug)]
pub enum SendTarget {
    Address(SocketAddr),
    Session(SessionId),
}

pub struct RequestDataRequest {
    pub target: SendTarget,
    pub id: Box<[u8]>,
    pub buffer: WriteableBuffer,
    pub sender: watch::Sender<StreamMessage>,
}

#[derive(Debug)]
pub struct SendDataRequest {
    pub target: SendTarget,
    pub buffer: ReadableBuffer,
    pub sender: watch::Sender<StreamMessage>,
}

pub enum ApiCommand {
    Connect(
        OneShot<
            SocketAddr,
            core::result::Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>,
        >,
    ),
    RejectTrackRequest(SessionId, Box<[u8]>),
    CloseSession(SessionId),
    CloseStream(SessionId),
    SetStreamComplete(SessionId, bool),
    FindHoles(OneShot<SessionId, Option<Vec<Range<usize>>>>),
    StreamAction(OneShot<(SessionId, PlaybackControl), EmptyResult>),
    RequestTrack(OneShot<RequestDataRequest, core::result::Result<SessionId, ApiErrors>>),
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
