use crate::{
    api::{
        self, SendTarget,
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
    ops::Range,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};

use futures::executor;

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
        executor::block_on(self.inner_close());
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

    pub(crate) async fn inner_close(&mut self) {
        if PROTOCOL_OPEN.load(Ordering::Relaxed) {
            let tx = self.api_to_manager.clone();
            _ = tx.send(ApiCommand::Close);
            if let Some(handle) = self.manager_handle.take() {
                _ = handle.join();
            }
            PROTOCOL_OPEN.store(false, Ordering::Relaxed);
        }
    }
}

impl ApiInner {
    pub fn close(mut self) {
        let tx = self.api_to_manager.clone();
        _ = tx.send(ApiCommand::Close);
        if let Some(handle) = self.manager_handle.take() {
            _ = handle.join();
        }
        PROTOCOL_OPEN.store(false, Ordering::Relaxed);
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
            None => Ok(InnerAppEvent::Closed),

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
                    ProtoError::FailedToDeref => {
                        InnerAppEvent::ProtocolFailed(ApiErrors::BufferClosedUnexpectedly)
                    }
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
    reply: Option<HandshakeDoneReceiver>,
}

impl Drop for PendingConnection {
    fn drop(&mut self) {
        executor::block_on(self.inner_discard())
    }
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
            reply: Some(reply),
        }
    }

    async fn inner_discard(&mut self) {
        let Ok(Ok((session_id, _))) = o_unwrap_or_return!(self.reply.take()).recv().await else {
            return;
        };

        let Some(api) = self.api.upgrade() else {
            return;
        };

        api.close_session(session_id).await;
    }
}

impl api::types::PendingConnection for PendingConnection {
    type Connection = Connection;
    type Error = ConnectionError;

    async fn ready(mut self) -> Result<Self::Connection, Self::Error> {
        let (session_id, receiver) = match self
            .reply
            .take()
            .expect("This is only removed on Drop or here")
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
            self.api.clone(),
            session_id,
            self.peer_address,
            receiver,
        ))
    }

    async fn discard(self) {}
}

#[derive(Debug)]
pub struct IncomingConnection {
    api: Weak<ApiInner>,
    peer_address: SocketAddr,
    app_id: AppId,
    approve_channel: Option<oneshot::Sender<AppResponse>>,
    reply: Option<HandshakeDoneReceiver>,
}

impl Drop for IncomingConnection {
    fn drop(&mut self) {
        executor::block_on(self.inner_reject("Dropped By Peer"));
    }
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
            reply: Some(reply),
        }
    }

    async fn inner_reject(&mut self, reason: impl Into<String>) {
        let sender = o_unwrap_or_return!(self.approve_channel.take());
        let reason = reason.into();
        _ = sender
            .send(AppResponse::AppRejected(reason))
            .panic_in_debug("ApiToManager channel failed sending in `reject`");
    }
}

impl api::types::IncomingConnection for IncomingConnection {
    type Connection = Connection;
    type Error = ConnectionError;

    fn app_id(&self) -> &str {
        &self.app_id
    }

    async fn reject(self) {}

    async fn approve(mut self) -> core::result::Result<Self::Connection, Self::Error> {
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

        match self
            .reply
            .take()
            .expect("This is only removed on Drop or here")
            .recv()
            .await
        {
            Err(e) => Err(ConnectionError::from_api(e)),
            Ok(Err(e)) => Err(e),
            Ok(Ok((session_id, receiver))) => Ok(Connection::new(
                self.api.clone(),
                session_id,
                self.peer_address,
                receiver,
            )),
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

    async fn inner_close(&self) {
        let api: Arc<ApiInner> = o_unwrap_or_return!(self.api.upgrade());
        api.close_session(self.session_id).await;
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        executor::block_on(self.inner_close());
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
            Ok(Ok(_)) => Ok(PendingStream::new_input(
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
        self.inner_close().await;
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
    connection: Option<Connection>,
    allow_partial: bool,
    update: watch::Receiver<StreamMessage>,
    _marker: PhantomData<Direction>,
}

pub type InputStream = Stream<api::types::Input>;
pub type OutputStream = Stream<api::types::Output>;

impl<Direction: api::types::StreamDirection> Drop for Stream<Direction> {
    fn drop(&mut self) {
        _ = executor::block_on(self.inner_close());
    }
}

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
            connection: Some(connection),
            allow_partial: false,
            update: receiver,
            _marker: PhantomData,
        }
    }

    async fn inner_close(&mut self) -> Result<Option<Connection>, (ConnectionError, Connection)> {
        let Some(conn) = self.connection.take() else {
            return Ok(None);
        };
        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, conn));
        };
        api.close_stream(self.session).await;
        Ok(Some(conn))
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
            Err(ApiErrors::BufferClosedUnexpectedly) => {
                Err(ConnectionError::BufferClosedUnexpectedly)
            }
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

    async fn wait_until_done(&mut self) -> Result<(), Self::Error> {
        self.update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)
            .map(|_| ())
    }

    async fn complete(mut self) -> Result<Self::Connection, Self::Error> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        api.complete_stream(self.session, self.allow_partial).await;

        if self
            .update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?
            .buffer_closed
        {
            // TODO: in the future just close stream
            Err(ConnectionError::BufferClosedUnexpectedly)
        } else {
            Ok(self
                .connection
                .take()
                .expect("This is only removed on Drop or here"))
        }
    }

    async fn close(mut self) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        self.inner_close()
            .await
            .map(|c| c.expect("Explicit call to close() will never have an empty connection field"))
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

    async fn wait_until_done(&mut self) -> Result<(), Self::Error> {
        self.update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)
            .map(|_| ())
    }

    async fn complete(mut self) -> Result<Self::Connection, Self::Error> {
        self.update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?;
        Ok(self
            .connection
            .take()
            .expect("This is only removed on Drop or here"))
    }

    async fn close(mut self) -> Result<Self::Connection, (Self::Error, Self::Connection)> {
        self.inner_close()
            .await
            .map(|c| c.expect("Explicit call to close() will never have an empty connection field"))
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
    connection: Option<Connection>,
    _marker: PhantomData<Direction>,
}

impl<Direction: api::types::StreamDirection> Drop for RequestedStream<Direction> {
    fn drop(&mut self) {
        _ = executor::block_on(self.inner_reject());
    }
}

impl<Direction: api::types::StreamDirection> RequestedStream<Direction> {
    async fn inner_reject(&mut self) -> Result<Option<Connection>, ConnectionError> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        api.reject_track_request(self.session, std::mem::take(&mut self.track_id))
            .await;
        Ok(self.connection.take())
    }
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
            connection: Some(connection),
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

    async fn reject(mut self) -> Result<Connection, Self::Error> {
        self.inner_reject()
            .await
            .map(|c| c.expect("Explicit call to reject() will always have a connection"))
    }

    async fn approve(
        mut self,
        buffer: impl Into<ReadableBuffer>,
    ) -> Result<Stream<api::types::Output>, (Self::Error, Connection)> {
        let conn = self
            .connection
            .take()
            .expect("This is only removed on Drop or here");

        let Some(api) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, conn));
        };

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
                conn,
                length,
                receiver,
            )),
            Err(e) => Err((ConnectionError::from_api(e), conn)),
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

    async fn approve(
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
    connection: Option<Connection>,
    _marker: PhantomData<Direction>,
}

impl<Direction: api::types::StreamDirection> PendingStream<Direction> {
    async fn inner_discard(
        &mut self,
    ) -> Result<Option<Connection>, (ConnectionError, Option<Connection>)> {
        let Some(_) = self.api.upgrade() else {
            return Err((ConnectionError::ProtocolClosed, self.connection.take()));
        };
        let Some(conn) = self.connection.take() else {
            return Ok(None);
        };
        let Some(conn) = self
            .inner_ready(conn)
            .await
            .map_err(|(e, c)| (e, Some(c)))?
            .inner_close()
            .await
            .map_err(|(e, c)| (e, Some(c)))?
        else {
            return Ok(None);
        };
        Ok(Some(conn))
    }

    async fn inner_ready(
        &mut self,
        conn: Connection,
    ) -> Result<Stream<Direction>, (ConnectionError, Connection)> {
        let Ok(r) = self.update.wait_for(|e| e.approved.is_some()).await else {
            return Err((ConnectionError::ProtocolClosed, conn));
        };

        if r.approved.is_some_and(identity) {
            drop(r);
            Ok(Stream::<Direction>::new(
                self.api.clone(),
                self.session,
                conn,
                self.stream_size,
                self.update.clone(),
            ))
        } else {
            Err((ConnectionError::PeerRejected(None), conn))
        }
    }
}

impl<Direction: api::types::StreamDirection> Drop for PendingStream<Direction> {
    fn drop(&mut self) {
        _ = executor::block_on(self.inner_discard());
    }
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
            connection: Some(connection),
            _marker: PhantomData,
        }
    }
}

impl api::types::PendingStream for PendingStream<api::types::Input> {
    type Stream = Stream<api::types::Input>;
    type Error = ConnectionError;
    type OwningConnection = Connection;

    async fn ready(mut self) -> Result<Stream<api::types::Input>, (Self::Error, Connection)> {
        let conn = self
            .connection
            .take()
            .expect("This is only removed on Drop or here");
        self.inner_ready(conn).await
    }

    async fn discard(mut self) -> Result<Connection, (Self::Error, Connection)> {
        match self.inner_discard().await {
            Ok(c) => Ok(c.expect("Explicit call to discard always has a connection")),
            Err((e, c)) => Err((
                e,
                c.expect("Explicit call to discard always has a connection"),
            )),
        }
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
