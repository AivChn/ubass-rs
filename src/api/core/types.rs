use crate::{
    api::{
        self, Api, SendTarget,
        types::{ReadableBuffer, WriteableBuffer},
    },
    error::{ConnectionError, EmptyResult},
    manager::{
        AppId, endpoints,
        packets::{BytePosition, MAX_PAYLOAD_LENGTH, PayloadField, PlaybackControlType, SessionId},
    },
    o_unwrap_or_return,
    prelude::{ApiCommand, ApiErrors, ApiMessage, AppResponse},
    utils::{
        ConnectionEvent, OneShot, PanicInDebug, PlaybackControl, RequestDataRequest,
        ResponseReceiver, SendDataRequest, StreamMessage,
    },
};
use core::result::Result;
use std::{
    marker::PhantomData,
    net::SocketAddr,
    range::Range,
    sync::{Arc, Weak, atomic::AtomicBool},
    thread::JoinHandle,
};

use aes_gcm_siv::Error;
use tokio::{
    runtime::Runtime,
    sync::{
        Mutex,
        mpsc::{self, Receiver, Sender},
        oneshot, watch,
    },
};

const CHANNEL_BUFFER_SIZE: usize = 256;

static PROTOCOL_OPEN: AtomicBool = AtomicBool::new(false);

pub type ApiToManager = Sender<ApiCommand>;
pub type ApiFromManager = Receiver<crate::prelude::Result<ApiMessage>>;

pub type ConnectionReceiver = Receiver<ConnectionEvent>;

pub struct ApiInner {
    manager_handle: Option<JoinHandle<Result<(), ApiErrors>>>,
    api_to_manager: ApiToManager,
    api_from_manager: Mutex<ApiFromManager>,
}

impl Drop for ApiInner {
    fn drop(&mut self) {
        let tx = self.api_to_manager.clone();
        _ = std::thread::spawn(move || tx.blocking_send(ApiCommand::Close)).join();
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
        _ = std::thread::spawn(move || sender.blocking_send(ApiCommand::Close)).join();
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
        ResponseReceiver<Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>>,
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
            Some(Err(_)) => {
                // TODO: surface specific protocol errors to the app
                Ok(InnerAppEvent::Closed)
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
    ) -> Result<SessionId, ApiErrors> {
        let track_id = track_id.into();
        if track_id.len() > MAX_PAYLOAD_LENGTH {
            return Err(ApiErrors::BufferTooLarge);
        }
        let (request, response) =
            OneShot::<RequestDataRequest, Result<SessionId, ApiErrors>>::new(RequestDataRequest {
                target,
                id: track_id,
                buffer: buffer.into(),
                sender,
            });
        self.api_to_manager
            .send(ApiCommand::RequestData(request))
            .await
            .map_err(|_| ApiErrors::ProtocolClosed)?;

        response.recv().await?
    }

    async fn close_stream(&self, session_id: SessionId) {
        _ = self
            .api_to_manager
            .send(ApiCommand::CloseStream(session_id))
            .await;
    }

    fn close_session(&self, session_id: SessionId) {
        // HACK: find a better way to handle this being syncronous
        _ = self
            .api_to_manager
            .try_send(ApiCommand::CloseSession(session_id));
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
}

#[derive(Debug)]
pub enum AppEvent {
    IncomingConnection(IncomingConnection),
    Closed,
}

pub enum InnerAppEvent {
    IncomingConnection {
        request: OneShot<AppId, AppResponse>,
        response: ResponseReceiver<
            core::result::Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>,
        >,
        peer_address: SocketAddr,
    },
    Closed,
}

type HandshakeDoneReceiver =
    ResponseReceiver<Result<(SessionId, Receiver<ConnectionEvent>), ConnectionError>>;

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

            api.close_session(session_id);
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
        api.close_session(self.session_id);
    }
}

impl api::types::Connection for Connection {
    type Event = ConnectionEvent;
    type Error = ConnectionError;
    type InputStream = InputStream;
    type OutputStream = OutputStream;

    async fn listen(&mut self) -> Result<Self::Event, Self::Error> {
        if self.api.strong_count() == 0 {
            let mut buffer = vec![];
            while let Some(message) = self.receiver.recv().await {
                buffer.push(message);
            }
            match buffer.last() {
                Some(ConnectionEvent::ProtocolClosed(_)) => {
                    Ok(ConnectionEvent::ProtocolClosed(buffer))
                }
                _ => Err(ConnectionError::ProtocolClosed),
            }
        } else {
            self.receiver
                .recv()
                .await
                .ok_or(ConnectionError::ProtocolClosed)
        }
    }

    #[allow(refining_impl_trait)]
    async fn send(self, buffer: impl Into<ReadableBuffer>) -> Result<OutputStream, Self::Error> {
        let api = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
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
            Err(e) => Err(ConnectionError::from_api(e)),
        }
    }

    async fn request(
        self,
        identifier: impl Into<Box<[u8]>>,
        buffer: impl Into<WriteableBuffer>,
    ) -> Result<InputStream, Self::Error> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        let buffer = buffer.into();
        let length = buffer.len();
        let (sender, receiver) = watch::channel(StreamMessage::default());
        match api
            .request_track(
                SendTarget::Session(self.session_id),
                identifier.into(),
                buffer,
                sender,
            )
            .await
        {
            Ok(_) => Ok(InputStream::new(
                self.api.clone(),
                self.session_id,
                self,
                length,
                receiver,
            )),
            Err(e) => Err(ConnectionError::from_api(e)),
        }
    }

    async fn close(self) {
        let api: Arc<ApiInner> = o_unwrap_or_return!(self.api.upgrade());
        api.close_session(self.session_id);
    }
}

#[derive(Debug)]
pub struct InputStream {
    api: Weak<ApiInner>,
    session: SessionId,
    stream_size: usize,
    connection: Connection,
    allow_partial: bool,
    update: watch::Receiver<StreamMessage>,
}

impl InputStream {
    pub(super) fn new(
        api: Weak<ApiInner>,
        session_id: SessionId,
        connection: Connection,
        stream_size: usize,
        receiver: watch::Receiver<StreamMessage>,
    ) -> Self {
        Self {
            api,
            session: session_id,
            stream_size,
            connection,
            allow_partial: false,
            update: receiver,
        }
    }
}

impl InputStream {
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

impl api::types::PlaybackControl for InputStream {
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

impl api::types::Stream for InputStream {
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
        self.update.borrow().head == self.stream_size || self.update.borrow().closed
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

    async fn close(self) -> Result<Self::Connection, Self::Error> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        api.close_stream(self.session).await;
        Ok(self.connection)
    }
}

#[allow(private_bounds)]
#[derive(Debug)]
pub struct OutputStream {
    api: Weak<ApiInner>,
    session: SessionId,
    stream_size: usize,
    connection: Connection,
    update: watch::Receiver<StreamMessage>,
}

impl OutputStream {
    pub(super) fn new(
        api: Weak<ApiInner>,
        session_id: SessionId,
        connection: Connection,
        stream_size: usize,
        receiver: watch::Receiver<StreamMessage>,
    ) -> Self {
        Self {
            api,
            session: session_id,
            connection,
            stream_size,
            update: receiver,
        }
    }
}

impl api::types::Stream for OutputStream {
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
        self.update.borrow().head == self.stream_size
    }

    async fn complete(mut self) -> Result<Self::Connection, Self::Error> {
        self.update
            .wait_for(|message| message.closed)
            .await
            .map_err(|_| ConnectionError::ProtocolClosed)?;
        Ok(self.connection)
    }

    async fn close(self) -> Result<Self::Connection, Self::Error> {
        let api: Arc<ApiInner> = self.api.upgrade().ok_or(ConnectionError::ProtocolClosed)?;
        api.close_stream(self.session).await;
        Ok(self.connection)
    }
}
