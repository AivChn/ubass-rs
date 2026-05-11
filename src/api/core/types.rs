use crate::{
    api::{
        self, SendTarget, StreamTrait,
        types::{ReadableBuffer, WriteableBuffer},
    },
    error::{
        ConnectionError, EmptyResult, Error as ProtoError, PacketProcessingError, TaskError,
        TransportError,
    },
    manager::{
        AppId, endpoints,
        packets::{MAX_PAYLOAD_LENGTH, SessionId},
    },
    o_unwrap_or_return,
    prelude::{ApiCommand, ApiErrors, ApiMessage, AppResponse},
    utils::{
        InnerConnectionEvent, OneShot, PanicInDebug, PlaybackControl, RequestDataRequest,
        ResponseReceiver, SendDataRequest, StreamMessage,
    },
};
use core::result::Result;
use std::{
    convert::identity,
    marker::PhantomData,
    net::SocketAddr,
    range::Range,
    sync::{Arc, Weak, atomic::AtomicBool},
    thread::JoinHandle,
};

use tokio::sync::{
    Mutex,
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};

const CHANNEL_BUFFER_SIZE: usize = 256;

static PROTOCOL_OPEN: AtomicBool = AtomicBool::new(false);

pub type ApiToManager = Sender<ApiCommand>;
pub type ApiFromManager = Receiver<crate::prelude::Result<ApiMessage>>;

pub type ConnectionReceiver = Receiver<InnerConnectionEvent>;

pub struct ApiInner {
    manager_handle: Option<JoinHandle<Result<(), ApiErrors>>>,
    api_to_manager: ApiToManager,
    api_from_manager: Mutex<ApiFromManager>,
}

impl Drop for ApiInner {
    fn drop(&mut self) {
        let tx = self.api_to_manager.clone();
        _ = std::thread::spawn(move || _ = tx.blocking_send(ApiCommand::Close)).join();
        if let Some(handle) = self.manager_handle.take() {
            _ = handle.join();
        }
        PROTOCOL_OPEN.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

impl ApiInner {
    pub fn new(port: u16, app_id: AppId) -> Result<Self, ApiErrors> {
        if PROTOCOL_OPEN.swap(true, std::sync::atomic::Ordering::Relaxed) {
            Err(ApiErrors::AlreadyOpen)
        } else {
            let (manager_to_api, api_from_manager) = mpsc::channel(CHANNEL_BUFFER_SIZE);
            let (api_to_manager, manager_from_api) = mpsc::channel(CHANNEL_BUFFER_SIZE);

            Ok(ApiInner {
                manager_handle: Some(endpoints::open(
                    port,
                    app_id,
                    manager_to_api,
                    manager_from_api,
                )?),
                api_to_manager,
                api_from_manager: Mutex::new(api_from_manager),
            })
        }
    }
}

impl ApiInner {
    pub fn close(mut self) -> Result<(), ApiErrors> {
        let sender = self.api_to_manager.clone();
        // blocking_send panics from within a tokio runtime — spawn a plain OS thread to avoid that
        _ = std::thread::spawn(move || _ = sender.blocking_send(ApiCommand::Close)).join();
        if let Some(handle) = self.manager_handle.take() {
            match handle.join() {
                Ok(res) => res?,
                Err(_) => return Err(ApiErrors::ThreadFailed("Manager")),
            }
        }
        PROTOCOL_OPEN.store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub async fn connect(
        &self,
        addr: std::net::SocketAddr,
    ) -> Result<
        ResponseReceiver<Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>>,
        ApiErrors,
    > {
        let (oneshot, reply) = OneShot::new(addr);
        self.api_to_manager
            .send(ApiCommand::Connect(oneshot))
            .await
            .map_err(|_| ApiErrors::ProtocolClosed)?;
        Ok(reply)
    }

    pub async fn listen(&self) -> Result<InnerAppEvent, ApiErrors> {
        match self.api_from_manager.lock().await.recv().await {
            // Channel sender dropped — clean shutdown.
            None => Ok(InnerAppEvent::Closed),

            // Manager surfaced an internal error. Two buckets:
            //   - Channel/Task/Transport: fatal infrastructure failures the
            //     app should be told about explicitly so it can decide what
            //     to do (retry, surface to its own user, etc.).
            //   - Anything else (peer-scoped PacketProcessor errors,
            //     manager-internal StateMismatch / IrrelevantError that
            //     leaked here): not protocol-wide; collapse to Closed so
            //     the app sees the same shape as a clean shutdown.
            Some(Err(error)) => {
                let event = match error {
                    ProtoError::Channel(_)
                    | ProtoError::Task(TaskError::TaskFailed)
                    | ProtoError::Transport(TransportError::RecvFailedTooManyTimes) => {
                        InnerAppEvent::ProtocolFailed(ApiErrors::Internal)
                    }
                    ProtoError::Transport(TransportError::FailedToBind) => {
                        InnerAppEvent::ProtocolFailed(ApiErrors::FailedToOpen)
                    }
                    ProtoError::PacketProcessor(PacketProcessingError::IncompatibleVersion(
                        _,
                        _,
                    ))
                    | ProtoError::StateMismatch { .. }
                    | ProtoError::IrrelevantError => InnerAppEvent::Closed,
                };
                Ok(event)
            }

            Some(Ok(ApiMessage::IncommingConncetion {
                request,
                response,
                peer_address,
            })) => {
                let event = InnerAppEvent::IncomingConnection {
                    request,
                    response,
                    peer_address,
                };
                Ok(event)
            }
        }
    }

    async fn send_data(
        &self,
        target: SendTarget,
        buffer: ReadableBuffer,
        sender: watch::Sender<StreamMessage>,
    ) -> Result<SessionId, ApiErrors> {
        let (request, reply) = OneShot::new(SendDataRequest {
            target,
            buffer,
            sender,
        });
        self.api_to_manager
            .send(ApiCommand::SendData(request))
            .await
            .map_err(|_| ApiErrors::ProtocolClosed)?;

        reply.recv().await?
    }

    async fn request_track(
        &self,
        target: SendTarget,
        track_id: impl Into<Box<[u8]>>,
        buffer: impl Into<WriteableBuffer>,
        sender: watch::Sender<StreamMessage>,
    ) -> ResponseReceiver<Result<SessionId, ApiErrors>> {
        let track_id = track_id.into();
        let (request, response) =
            OneShot::<RequestDataRequest, Result<SessionId, ApiErrors>>::new(RequestDataRequest {
                target,
                id: track_id,
                buffer: buffer.into(),
                sender,
            });
        _ = self
            .api_to_manager
            .send(ApiCommand::RequestTrack(request))
            .await;

        response
    }

    async fn close_stream(&self, session_id: SessionId) {
        _ = self
            .api_to_manager
            .send(ApiCommand::CloseStream(session_id))
            .await;
    }

    async fn close_session(&self, session_id: SessionId) {
        _ = self
            .api_to_manager
            .send(ApiCommand::CloseSession(session_id))
            .await;
    }

    async fn complete_stream(&self, session_id: SessionId, allow_partial: bool) {
        _ = self
            .api_to_manager
            .send(ApiCommand::SetStreamComplete(session_id, allow_partial))
            .await;
    }

    async fn find_holes(
        &self,
        session_id: SessionId,
    ) -> ResponseReceiver<Option<Vec<Range<usize>>>> {
        let (request, response) = OneShot::new(session_id);
        _ = self
            .api_to_manager
            .send(ApiCommand::FindHoles(request))
            .await;
        response
    }

    async fn send_playback_control(
        &self,
        session_id: SessionId,
        control: PlaybackControl,
    ) -> ResponseReceiver<EmptyResult> {
        let (sender, receiver) = OneShot::new((session_id, control));
        _ = self
            .api_to_manager
            .send(ApiCommand::StreamAction(sender))
            .await;
        receiver
    }

    async fn reject_track_request(&self, session_id: SessionId, track_id: Box<[u8]>) {
        _ = self
            .api_to_manager
            .send(ApiCommand::RejectTrackRequest(session_id, track_id))
            .await;
    }
}

#[derive(Debug)]
pub enum AppEvent {
    IncomingConnection(IncomingConnection),
    /// Protocol shut down cleanly (or for a reason the app needn't act on
    /// — peer-scoped or manager-internal anomaly).
    Closed,
    /// Protocol stopped due to an infrastructure-level failure. The reason
    /// gives the app what it needs to log, branch, or surface to its own
    /// caller. After this event the API instance will not produce further
    /// events.
    ProtocolFailed(ApiErrors),
}

pub enum InnerAppEvent {
    IncomingConnection {
        request: OneShot<AppId, AppResponse>,
        response: ResponseReceiver<
            core::result::Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>,
        >,
        peer_address: SocketAddr,
    },
    Closed,
    ProtocolFailed(ApiErrors),
}

type HandshakeDoneReceiver =
    ResponseReceiver<Result<(SessionId, Receiver<InnerConnectionEvent>), ConnectionError>>;

pub struct PendingConnection {
    api: Weak<ApiInner>,
    peer_address: SocketAddr,
    reply: HandshakeDoneReceiver,
}

impl PendingConnection {
    pub(super) fn new(
        api: Weak<ApiInner>,
        peer_address: SocketAddr,
        reply: HandshakeDoneReceiver,
    ) -> Self {
        Self {
            api,
            peer_address,
            reply,
        }
    }
}

impl api::types::PendingConnection for PendingConnection {
    type Connection = Connection;
    type Error = ConnectionError;

    async fn ready(self) -> Result<Self::Connection, Self::Error> {
        let (session_id, receiver) = match self
            .reply
            .recv()
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?
        {
            Ok(v) => v,
            Err(e) => {
                return Err(e);
            }
        };
        Ok(Connection::new(
            self.api,
            session_id,
            self.peer_address,
            receiver,
        ))
    }

    // HACK: This is a temporary solution. idealy, the pending connection could tell the protocol to
    // mark it as stale
    async fn discard(self) -> Result<(), Self::Error> {
        if self.api.strong_count() == 0 {
            return Err(ConnectionError::ProtocolClosed);
        }
        tokio::spawn(async move {
            let Ok(Ok((session_id, _))) = self.reply.recv().await else {
                return;
            };

            let Some(api) = self.api.upgrade() else {
                return;
            };

            api.close_session(session_id).await;
        });

        Ok(())
    }
}

#[derive(Debug)]
pub struct IncomingConnection {
    api: Weak<ApiInner>,
    peer_address: SocketAddr,
    app_id: AppId,
    approve_channel: Option<oneshot::Sender<AppResponse>>,
    reply: HandshakeDoneReceiver,
}

impl IncomingConnection {
    #[must_use]
    pub fn new(
        api: Weak<ApiInner>,
        peer_address: SocketAddr,
        app_id: AppId,
        approve_channel: oneshot::Sender<AppResponse>,
        reply: HandshakeDoneReceiver,
    ) -> Self {
        Self {
            api,
            peer_address,
            app_id,
            approve_channel: Some(approve_channel),
            reply,
        }
    }
}

impl api::types::IncomingConnection for IncomingConnection {
    type Connection = Connection;
    type Error = ConnectionError;

    fn app_id(&self) -> &str {
        &self.app_id
    }

    async fn reject(mut self, reason: impl Into<String>) -> Result<(), Self> {
        let Some(sender) = self.approve_channel.take() else {
            return Ok(());
        };
        let reason = reason.into();
        if reason.len() > MAX_PAYLOAD_LENGTH || !reason.is_ascii() {
            return Err(self);
        }
        _ = sender
            .send(AppResponse::AppRejected(reason))
            .panic_in_debug("ApiToManager channel failed sending in `reject`");
        Ok(())
    }

    async fn approve_and_ready(mut self) -> core::result::Result<Self::Connection, Self::Error> {
        let sender = self.approve_channel.take();
        debug_assert!(
            sender.is_some(),
            "Invariant broken while approving app ID ({}): `approve_channel` is `None`",
            *self.app_id
        );
        let sender = sender.ok_or(ConnectionError::ProtocolClosed)?;

        sender
            .send(AppResponse::AppApproved)
            .map_err(|_| ConnectionError::ProtocolClosed)?;

        match self.reply.recv().await {
            Err(e) => Err(ConnectionError::from_api(e)),
            Ok(Err(e)) => Err(e),
            Ok(Ok((session_id, receiver))) => Ok(Connection::new(
                self.api,
                session_id,
                self.peer_address,
                receiver,
            )),
        }
    }

    async fn approve_if_and_ready(
        self,
        condition: impl FnOnce(&str) -> bool,
        rejection_reason: impl Into<String>,
    ) -> Option<core::result::Result<Self::Connection, Self::Error>> {
        if condition(&self.app_id) {
            Some(self.approve_and_ready().await)
        } else {
            let reason = rejection_reason.into();
            if !reason.is_ascii() || reason.len() > MAX_PAYLOAD_LENGTH {
                Some(Err(ConnectionError::InvalidReason(self)))
            } else {
                _ = self.reject(reason).await;
                None
            }
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    api: Weak<ApiInner>,
    session_id: SessionId,
    peer_address: SocketAddr,
    receiver: ConnectionReceiver,
}

impl Connection {
    pub(super) fn new(
        api: Weak<ApiInner>,
        session_id: SessionId,
        peer_address: SocketAddr,
        receiver: ConnectionReceiver,
    ) -> Self {
        Self {
            api,
            session_id,
            peer_address,
            receiver,
        }
    }

    #[must_use]
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    #[must_use]
    pub fn peer(&self) -> SocketAddr {
        self.peer_address
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let api: Arc<ApiInner> = o_unwrap_or_return!(self.api.upgrade());
        let session_id = self.session_id;
        let tx = api.api_to_manager.clone();
        _ = std::thread::spawn(move || _ = tx.blocking_send(ApiCommand::CloseSession(session_id)))
            .join();
    }
}

impl api::types::Connection for Connection {
    type Event = ConnectionEvent;
    type Error = ConnectionError;
    type InputStream = InputStream;
    type OutputStream = OutputStream;
    type PendingInputStream = PendingStream<api::types::Input>;

    async fn listen(mut self) -> Result<Self::Event, Self::Error> {
        let inner = if self.api.strong_count() == 0 {
            let mut last = Box::new(None);
            while let Some(message) = self.receiver.recv().await {
                last.replace(message);
            }
            match *last {
                Some(InnerConnectionEvent::ProtocolClosed) => InnerConnectionEvent::ProtocolClosed,
                _ => return Err(ConnectionError::ProtocolClosed),
            }
        } else {
            self.receiver
                .recv()
                .await
                .ok_or(ConnectionError::ProtocolClosed)?
        };

        let event = match inner {
            InnerConnectionEvent::TrackRequest(track_id) => {
                let api = self.api.clone();
                let session_id = self.session_id;
                ConnectionEvent::TrackRequested(RequestedStream::new_output(
                    api, session_id, track_id, self,
                ))
            }
            InnerConnectionEvent::ProtocolClosed => ConnectionEvent::ProtocolClosed,
            InnerConnectionEvent::ConnectionClosed => ConnectionEvent::ConnectionClosed,
        };
        Ok(event)
    }

    #[allow(refining_impl_trait)]
    async fn send(
        self,
        buffer: impl Into<ReadableBuffer>,
    ) -> Result<OutputStream, (Self::Error, Self)> {
        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self));
        };
        let buffer = buffer.into();
        let length = buffer.len();
        let (sender, receiver) = watch::channel(StreamMessage::default());
        match api
            .send_data(SendTarget::Session(self.session_id), buffer, sender)
            .await
        {
            Ok(_) => Ok(OutputStream::new(
                self.api.clone(),
                self.session_id,
                self,
                length,
                receiver,
            )),
            Err(e) => Err((ConnectionError::from_api(e), self)),
        }
    }

    async fn request(
        self,
        identifier: impl Into<Box<[u8]>>,
        buffer: impl Into<WriteableBuffer>,
    ) -> Result<PendingStream<api::types::Input>, (Self::Error, Self)> {
        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self));
        };

        let api: Arc<ApiInner> = api;

        let track_id = identifier.into();
        if track_id.len() > MAX_PAYLOAD_LENGTH {
            return Err((ConnectionError::BufferTooLarge, self));
        }

        let buffer = buffer.into();
        let buffer_len = buffer.len();
        let _length = buffer.len();
        let (sender, receiver) = watch::channel(StreamMessage::default());

        let response = api
            .request_track(
                SendTarget::Session(self.session_id),
                track_id,
                buffer,
                sender,
            )
            .await;

        match response.recv().await {
            Ok(Ok(_s)) => Ok(PendingStream::new_input(
                self.api.clone(),
                self.session_id,
                buffer_len,
                receiver,
                self,
            )),
            Ok(Err(e)) => {
                let error = match e {
                    // These two should be impossible
                    ApiErrors::NoFreeSession | ApiErrors::SessionOccupied => {
                        ConnectionError::UnknownInternalError
                    }
                    ApiErrors::SessionDoesNotExist => ConnectionError::SessionClosedByPeer,
                    _ => ConnectionError::ProtocolClosed,
                };
                Err((error, self))
            }
            Err(_) => Err((ConnectionError::ProtocolClosed, self)),
        }
    }

    async fn close(self) {
        let api: Arc<ApiInner> = o_unwrap_or_return!(self.api.upgrade());
        api.close_session(self.session_id).await;
    }
}

/// Direction-parameterized stream type. Common fields live here once;
/// direction-specific behavior is supplied via per-direction trait impls
/// (`impl api::types::Stream for Stream<Input>`, etc.). `allow_partial` is
/// only meaningful for the `Input` direction (it's read in `Stream<Input>`'s
/// `complete`); `Output` ignores it.
#[allow(private_bounds)]
#[derive(Debug)]
pub struct Stream<Direction: api::types::StreamDirection> {
    api: Weak<ApiInner>,
    session: SessionId,
    size: usize,
    connection: Connection,
    allow_partial: bool,
    update: watch::Receiver<StreamMessage>,
    _marker: PhantomData<Direction>,
}

pub type InputStream = Stream<api::types::Input>;
pub type OutputStream = Stream<api::types::Output>;

impl<Direction: api::types::StreamDirection> Stream<Direction> {
    pub(super) fn new(
        api: Weak<ApiInner>,
        session_id: SessionId,
        connection: Connection,
        size: usize,
        receiver: watch::Receiver<StreamMessage>,
    ) -> Self {
        Self {
            api,
            session: session_id,
            size,
            connection,
            allow_partial: false,
            update: receiver,
            _marker: PhantomData,
        }
    }
}

impl Stream<api::types::Input> {
    async fn send_playback_control(
        &self,
        control: PlaybackControl,
    ) -> Result<usize, ConnectionError> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        // waits for the packet to be sent and the state to be actually updated
        match api
            .send_playback_control(self.session, control)
            .await
            .recv()
            .await
        {
            Ok(Err(())) | Err(_) => Err(ConnectionError::ProtocolClosed),
            _ => Ok(self.update.borrow().head),
        }
    }

    pub fn allow_partial_receive(&mut self, allow: bool) {
        self.allow_partial = allow;
    }

    pub async fn buffer_holes(&self) -> Result<Vec<Range<usize>>, ConnectionError> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        let response = api.find_holes(self.session).await;
        response
            .recv()
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?
            .ok_or(ConnectionError::ProtocolClosed)
    }
}

impl api::types::PlaybackControl for Stream<api::types::Input> {
    async fn pause(&self) -> Result<usize, Self::Error> {
        self.send_playback_control(PlaybackControl::Pause).await
    }

    async fn play(&self) -> Result<usize, Self::Error> {
        self.send_playback_control(PlaybackControl::Play).await
    }

    async fn seek(&self, position: usize) -> Result<usize, Self::Error> {
        self.send_playback_control(PlaybackControl::Seek(position.into()))
            .await
    }
}

impl api::types::Stream for Stream<api::types::Input> {
    type Error = ConnectionError;
    type Idx = usize;
    type Connection = Connection;

    fn current_position(&self) -> usize {
        self.update.borrow().head
    }

    fn is_playing(&self) -> bool {
        !self.update.borrow().paused
    }

    async fn is_done(&self) -> bool {
        self.update.borrow().head == self.size || self.update.borrow().closed
    }

    async fn complete(mut self) -> Result<Self::Connection, Self::Error> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        api.complete_stream(self.session, self.allow_partial).await;

        self.update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?;
        Ok(self.connection)
    }

    async fn close(self) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self.connection));
        };
        api.close_stream(self.session).await;
        Ok(self.connection)
    }
}

impl api::types::Stream for Stream<api::types::Output> {
    type Error = ConnectionError;
    type Idx = usize;
    type Connection = Connection;

    fn current_position(&self) -> usize {
        self.update.borrow().head
    }

    fn is_playing(&self) -> bool {
        !self.update.borrow().paused
    }

    async fn is_done(&self) -> bool {
        self.update.borrow().head == self.size
    }

    async fn complete(mut self) -> Result<Self::Connection, Self::Error> {
        self.update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?;
        Ok(self.connection)
    }

    async fn close(self) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self.connection));
        };
        api.close_stream(self.session).await;
        Ok(self.connection)
    }
}

// ============================================================================
// Public ConnectionEvent + RequestedStream<Direction> + PendingStream<Direction>
// ============================================================================

/// Public event yielded by [`Connection::listen()`]. Internally the connection
/// channel carries `InnerConnectionEvent` (primitives + fingerprint); this
/// enum is the API-facing wrapping with typed handles.
#[derive(Debug)]
pub enum ConnectionEvent {
    TrackRequested(RequestedStream<api::types::Output>),
    ProtocolClosed,
    ConnectionClosed,
}

#[allow(private_bounds)]
#[derive(Debug)]
pub struct RequestedStream<Direction: api::types::StreamDirection> {
    api: Weak<ApiInner>,
    session: SessionId,
    track_id: Box<[u8]>,
    connection: Connection,
    _marker: PhantomData<Direction>,
}

impl RequestedStream<api::types::Output> {
    pub(crate) fn new_output(
        api: Weak<ApiInner>,
        session: SessionId,
        track_id: Box<[u8]>,
        connection: Connection,
    ) -> Self {
        Self {
            api,
            session,
            track_id,
            connection,
            _marker: PhantomData,
        }
    }
}

impl api::types::RequestedStream for RequestedStream<api::types::Output> {
    type Stream = Stream<api::types::Output>;
    type Error = ConnectionError;
    type OwningConnection = Connection;
    type ApprovalBuffer = ReadableBuffer;

    fn track_id(&self) -> &[u8] {
        &self.track_id
    }

    async fn reject(self) -> Result<Connection, Self::Error> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        api.reject_track_request(self.session, self.track_id).await;
        Ok(self.connection)
    }

    async fn approve_and_ready(
        self,
        buffer: impl Into<ReadableBuffer>,
    ) -> Result<Stream<api::types::Output>, (Self::Error, Connection)> {
        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self.connection));
        };
        let api: Arc<ApiInner> = api;

        let buffer = buffer.into();
        let length = buffer.len();
        let (sender, receiver) = watch::channel(StreamMessage::default());
        match api
            .send_data(SendTarget::Session(self.session), buffer, sender)
            .await
        {
            Ok(_) => Ok(Stream::<api::types::Output>::new(
                self.api.clone(),
                self.session,
                self.connection,
                length,
                receiver,
            )),
            Err(e) => Err((ConnectionError::from_api(e), self.connection)),
        }
    }
}

// `RequestedStream<Input>` (peer offers data, we receive) is not yet
// implemented in the protocol — kept as a typestate placeholder for when
// that flow is wired.
#[allow(clippy::todo)]
impl api::types::RequestedStream for RequestedStream<api::types::Input> {
    type Stream = Stream<api::types::Input>;
    type Error = ConnectionError;
    type OwningConnection = Connection;
    type ApprovalBuffer = WriteableBuffer;

    fn track_id(&self) -> &[u8] {
        &self.track_id
    }

    async fn reject(self) -> Result<Connection, Self::Error> {
        todo!("RequestedStream<Input> not yet wired")
    }

    async fn approve_and_ready(
        self,
        _buffer: impl Into<WriteableBuffer>,
    ) -> Result<Stream<api::types::Input>, (Self::Error, Connection)> {
        todo!("RequestedStream<Input> not yet wired")
    }
}

#[allow(private_bounds)]
#[derive(Debug)]
pub struct PendingStream<Direction: api::types::StreamDirection> {
    api: Weak<ApiInner>,
    session: SessionId,
    stream_size: usize,
    update: watch::Receiver<StreamMessage>,
    connection: Connection,
    _marker: PhantomData<Direction>,
}

impl PendingStream<api::types::Input> {
    pub(crate) fn new_input(
        api: Weak<ApiInner>,
        session: SessionId,
        stream_size: usize,
        update: watch::Receiver<StreamMessage>,
        connection: Connection,
    ) -> Self {
        Self {
            api,
            session,
            stream_size,
            update,
            connection,
            _marker: PhantomData,
        }
    }
}

impl api::types::PendingStream for PendingStream<api::types::Input> {
    type Stream = Stream<api::types::Input>;
    type Error = ConnectionError;
    type OwningConnection = Connection;

    async fn ready(mut self) -> Result<Stream<api::types::Input>, (Self::Error, Connection)> {
        let Ok(r) = self.update.wait_for(|e| e.approved.is_some()).await else {
            return Err((ConnectionError::ProtocolClosed, self.connection));
        };

        if r.approved.is_some_and(identity) {
            drop(r);
            Ok(Stream::<api::types::Input>::new(
                self.api,
                self.session,
                self.connection,
                self.stream_size,
                self.update,
            ))
        } else {
            Err((ConnectionError::PeerRejected(None), self.connection))
        }
    }

    async fn discard(self) -> Result<Connection, (Self::Error, Connection)> {
        let Some(_api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self.connection));
        };
        let stream = self.ready().await?;
        stream.close().await
    }
}

// `PendingStream<Output>` has no use case in the current protocol model
// (`Connection::send` already returns the live OutputStream synchronously
// since there's no per-stream peer-approval step on the sender side).
// Kept as a typestate placeholder for symmetry.
#[allow(clippy::todo)]
impl api::types::PendingStream for PendingStream<api::types::Output> {
    type Stream = Stream<api::types::Output>;
    type Error = ConnectionError;
    type OwningConnection = Connection;

    async fn ready(self) -> Result<Stream<api::types::Output>, (Self::Error, Connection)> {
        todo!("PendingStream<Output> not yet wired")
    }

    async fn discard(self) -> Result<Connection, (Self::Error, Self::OwningConnection)> {
        todo!("PendingStream<Output> not yet wired")
    }
}
