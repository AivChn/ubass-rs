use std::net::SocketAddr;
use std::ops::Range;

use crate::api::{ReadableBuffer, WriteableBuffer};
use crate::error::ApiErrors;
use crate::manager::packets::{
    BytePosition, ByteRange, FecConfig, PlaybackControlPacket, PlaybackControlType,
};

use derive_more::Display;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, watch};

use crate::manager::AppId;
use crate::prelude::*;
use crate::{
    manager::packets::{PacketWrapper, SessionId},
    packet_processor::types::ProcessedPacket,
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
    /// `(track_id, fec_config)`. `fec_config` is the FEC strategy the
    /// requesting peer wants for the response stream; the app should pass
    /// it through to its `send_data` so the chosen scheme is honoured.
    TrackRequest(Box<[u8]>, FecConfig),
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
    pub buffer_closed: bool,
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
    /// FEC strategy the requester wants the responding peer to use when
    /// sending the requested data back. Travels in the body of the
    /// outbound `TrackRequestPacket`.
    pub fec_config: FecConfig,
}

#[derive(Debug)]
pub struct SendDataRequest {
    pub target: SendTarget,
    pub buffer: ReadableBuffer,
    pub sender: watch::Sender<StreamMessage>,
    /// FEC strategy chosen by the sending app for this stream. Stamped on
    /// each outbound data packet via `FECInfo`.
    pub fec_config: FecConfig,
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
    /// Drain the data-collection entries accumulated for one session. The
    /// reply contains every completed entry (including the open window,
    /// flushed at drain time) since the last drain. Empty on unknown
    /// sessions and on collector shutdown.
    DrainData(OneShot<SessionId, Vec<crate::utils::DataEntry>>),
    Close,
}

impl<Req: Send> OneShot<Req, core::result::Result<(), ApiErrors>> {
    pub fn command(value: Req) -> (Self, ResponseReceiver<core::result::Result<(), ApiErrors>>) {
        OneShot::new(value)
    }
}

pub enum ManagerMessage {
    Packet(PacketWrapper),
    Closed,
}

#[derive(thiserror::Error, Debug, Display)]
pub struct CouldNotRecover;

#[derive(Debug)]
pub enum PacketProcessingMessage {
    SendPacket(PacketWrapper),
    ReceivedPacket(ReceivedPacket),
    Close,
    Closed,
}

pub enum TransportMessage {
    SendPacket(ProcessedPacket),
    Close,
}
