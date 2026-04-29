#![allow(private_interfaces)]
use aes_gcm_siv::Aes256GcmSiv;
use derive_more::{Deref, Display};
use reed_solomon_simd::engine::tables;
use tokio::{
    select,
    sync::{Mutex, RwLock, mpsc, oneshot, watch},
    time::interval,
};
use x25519_dalek::EphemeralSecret;

use crate::{
    api::{ReadableBuffer, WriteableBuffer},
    debug_match_or_return, get_state, lock, lock_read, lock_write,
    manager::{
        CHANNEL_BUFFER_SIZE, STATE,
        packets::{
            BatchID, BytePosition, ByteRange, DataPacket, FECInfo, MAX_PAYLOAD_LENGTH, Options,
            Packet, PacketFingerprint, SessionId,
        },
        types::{ForeignTimestamp, ManagerToProcessor},
    },
    match_or_return, o_unwrap_or_return,
    packet_processor::fec::inference,
    prelude::*,
    r_unwrap_or_return,
};
use core::panic;
use std::{
    collections::{HashSet, VecDeque, hash_map::Entry},
    fmt::Display,
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

const PACKET_DISCARD_TIME_MS: u64 = 7 * 1000;
const SEND_TIMEOUT: u64 = 25;
const PACKET_COUNT_PER_BATCH: usize = 5;

macro_rules! sessions_state_fields {
    ($($name:ident($key:ty => $value:ty)),*) => {
        $(
            #[derive(Default, Deref)]
            pub struct $name(RwLock<HashMap<$key, $value>>);
        )*
    };
}

sessions_state_fields!(
    GeneralStateTable(SessionId => GeneralSessionState),
    EncryptionTable(SessionId => EncryptionWindow),
    FingerprintTable(SessionId => Arc<FingerprintWindow>),
    FecStateTable(SessionId => SessionFecState),
    SessionAppIdTable(SessionId => AppId),
    SessionAddressTable(SessionId => SocketAddr),
    HandshakeStateTable(HandshakeId => HandshakeState),
    AddressSessionIdTable(SocketAddr => Vec<SessionId>)
);

#[derive(Default)]
pub struct LastActivityTable(HashMap<SessionId, ForeignTimestamp>);

#[derive(Default, Debug, Deref)]
pub struct ConnectionStatesTable(RwLock<HashMap<SessionId, ConnectionStates>>);

impl ConnectionStatesTable {
    pub async fn address(&self, session_id: SessionId) -> Option<SocketAddr> {
        match lock_read!(self.0).get(&session_id)? {
            ConnectionStates::Handshake { address, .. } => Some(*lock_read!(address)),
            ConnectionStates::Established(box EstablishedState { address, .. }) => {
                Some(*lock_read!(address))
            }
        }
    }
}

#[derive(Debug)]
pub struct StreamingTo {
    pub buffer: ReadableBuffer,
    pub event: Arc<Shared<StreamEvent>>,
}

#[derive(Debug)]
pub enum Streaming {
    To(StreamingTo),
    From(WriteableBuffer),
}

#[derive(Debug)]
pub struct StreamState {
    pub streaming: Streaming,
    pub stream: watch::Sender<StreamMessage>,
    pub fec: SessionFecState,
}

impl StreamState {
    pub fn get_chunks(&mut self, n: usize) -> Option<Vec<(BytePosition, Box<[u8]>)>> {
        if let Streaming::To(StreamingTo { buffer, .. }) = &mut self.streaming
            && !self.stream.borrow().paused
            && !self.stream.borrow().closed
        {
            let v: Vec<_> = buffer.take(n).collect();
            self.stream.send_modify(|s| {
                s.head = (s.head + v.len() * MAX_PAYLOAD_LENGTH).min(buffer.len());
            });
            Some(v)
        } else {
            None
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn retransmit(&mut self, ranges: Vec<ByteRange>) -> Option<Vec<(BytePosition, Box<[u8]>)>> {
        if let Streaming::To(StreamingTo { buffer, .. }) = &mut self.streaming
            && !self.stream.borrow().paused
            && !self.stream.borrow().closed
        {
            let mut buf = vec![];
            for mut range in ranges {
                for _ in 0..=(range.length as usize / MAX_PAYLOAD_LENGTH) {
                    let end = (*range.start as usize + MAX_PAYLOAD_LENGTH).min(buffer.len());
                    let Some(payload) = buffer.read(*range.start as usize..end) else {
                        return Some(buf);
                    };

                    buf.push((range.start, Box::from(payload)));

                    *range.start += MAX_PAYLOAD_LENGTH as u32;
                }
            }
            Some(buf)
        } else {
            None
        }
    }

    pub fn close(&mut self) {
        self.stream.send_modify(|s| s.closed = true);
    }

    pub fn start(&mut self) {
        self.stream.send_modify(|s| s.paused = false);
    }

    pub fn pause(&mut self) {
        self.stream.send_modify(|s| s.paused = true);
    }
}

#[derive(Debug)]
pub enum SessionStates {
    Up,
    Down,
    Streaming(StreamState),
}

impl Display for SessionStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            SessionStates::Up => "Up",
            SessionStates::Down => "Down",
            SessionStates::Streaming(StreamState {
                streaming: Streaming::To { .. },
                ..
            }) => "StreamingTo",
            SessionStates::Streaming(StreamState {
                streaming: Streaming::From(_),
                ..
            }) => "StreamingFrom",
        };
        write!(f, "{name}")
    }
}

#[derive(Debug)]
pub struct EstablishedState {
    pub last_activity: Mutex<ForeignTimestamp>,
    pub connection: mpsc::Sender<ConnectionEvent>,
    pub state: SessionStates,
    pub address: RwLock<SocketAddr>,
    pub app_id: AppId,
}

#[derive(Debug)]
pub enum ConnectionStates {
    Handshake {
        ack_triggered_response: oneshot::Sender<
            core::result::Result<(SessionId, mpsc::Receiver<ConnectionEvent>), ConnectionError>,
        >,
        signal: watch::Sender<bool>,
        app_id: AppId,
        address: RwLock<SocketAddr>,
    },
    Established(Box<EstablishedState>),
}

impl ConnectionStates {
    /// # Panics
    pub fn streaming_from(
        &mut self,
        buffer: WriteableBuffer,
        sender: watch::Sender<StreamMessage>,
    ) {
        match self {
            Self::Established(box EstablishedState {
                state: state @ (SessionStates::Up | SessionStates::Down),
                ..
            }) => {
                *state = SessionStates::Streaming(StreamState {
                    streaming: Streaming::From(buffer),
                    stream: sender,
                    fec: SessionFecState::default(),
                });
            }

            Self::Established(box EstablishedState {
                state,
                address,
                app_id,
                ..
            }) => {
                debug_assert!(
                    false,
                    "Invariant broken while trying to stream from {} with app_id {}: Session was not free, instead being in state {}",
                    *address.try_read().unwrap_or_else(|_| panic!(
                        "Invariant broken while trying to stream with app_id {app_id}: \
                        Session was not free, instead being in state {state}, \
                        as well as failed to get the address from the RwLock: {address:?}"
                    )),
                    app_id,
                    state
                );
            }
            Self::Handshake {
                app_id, address, ..
            } => {
                debug_assert!(false,
                    "Invariant broken while trying to stream from {} with app_id {}: This session is not fully Established yet.",
                address.try_read().unwrap_or_else(|_| panic!("Invariant broken while trying to stream with app_id {app_id}: \
                    This session is not fully Established yet. Address failed to get lock from RwLock: {address:?}")),
                    app_id);
            }
        }
    }

    /// # Panics
    pub fn stream_to(&mut self, buffer: ReadableBuffer, sender: watch::Sender<StreamMessage>) {
        match self {
            Self::Established(box EstablishedState {
                state: state @ (SessionStates::Up | SessionStates::Down),
                ..
            }) => {
                *state = SessionStates::Streaming(StreamState {
                    streaming: Streaming::To(StreamingTo {
                        buffer,
                        event: Arc::default(),
                    }),
                    stream: sender,
                    fec: SessionFecState::default(),
                });
            }

            Self::Established(box EstablishedState {
                state,
                address,
                app_id,
                ..
            }) => {
                debug_assert!(
                    false,
                    "Invariant broken while trying to stream to {} with app_id {}: Session was not free, instead being in state {}",
                    *address.try_read().unwrap_or_else(|_| panic!(
                        "Invariant broken while trying to stream with app_id {app_id}: \
                        Session was not free, instead being in state {state}, \
                        as well as failed to get the address from the RwLock: {address:?}"
                    )),
                    app_id,
                    state
                );
            }
            Self::Handshake {
                app_id, address, ..
            } => {
                debug_assert!(false,
                    "Invariant broken while trying to stream to {} with app_id {}: This session is not fully Established yet.",
                address.try_read().unwrap_or_else(|_| panic!("Invariant broken while trying to stream with app_id {app_id}: \
                    This session is not fully Established yet. Address failed to get lock from RwLock: {address:?}")),
                    app_id);
            }
        }
    }

    pub async fn close_stream(&mut self) {
        let ConnectionStates::Established(box EstablishedState {
            state: state @ SessionStates::Streaming(_),
            ..
        }) = self
        else {
            debug_assert!(
                false,
                "Invariant broken in `close_stream`: function has been called on a session with no open stream"
            );
            return;
        };

        let SessionStates::Streaming(stream_state) = state else {
            unreachable!("Any other arm has been handled by the let else statement above");
        };

        if let Streaming::To(StreamingTo { event, .. }) = &stream_state.streaming {
            event.update(StreamEvent::Close).await;
        }

        stream_state.stream.send_modify(|m| m.closed = true);

        *state = SessionStates::Up;
    }

    // TODO:
    /// # Errors
    pub fn received_data_packet(&mut self, packet: DataPacket) -> ErrResult {
        if let ConnectionStates::Established(box EstablishedState {
            state:
                SessionStates::Streaming(StreamState {
                    streaming: Streaming::From(buffer),
                    stream,
                    ..
                }),
            ..
        }) = self
        {
            let payload = packet.payload.take();
            buffer
                .write(packet.byte_range_start, payload)
                .ok_or(ChannelError::ChannelClosed(Inbound))?;
            stream.send_modify(|m| m.head = buffer.head());
            if buffer.is_done() {
                stream.send_modify(|m| m.closed = true);
            }

            Ok(())
        } else {
            Err(Error::StateMismatch {
                expected: FlatState::StreamingFrom,
                found: (&*self).into(),
            })
        }
    }
}

pub struct ProtocolState {
    app_id: AppId,
    port: Port,
    // this is a mutex because the compiler hates me specifically
    handles: Mutex<Option<LayerHandles>>,
    pub global_handle_monitor: Arc<HandleMonitor>,
    pub connections: ConnectionStatesTable,
    pub handshakes: HandshakeStateTable,
    pub ack: PendingAckWindow,
    pub encryption: EncryptionTable,
    pub fingerprints: FingerprintTable,
    pub address_session: AddressSessionIdTable,
}

impl ProtocolState {
    #[must_use]
    pub fn new(port: Port, app_id: AppId, sender: ManagerToProcessor) -> Self {
        let global_handle_monitor = Arc::new(HandleMonitor::default());
        global_handle_monitor.clone().init();

        Self {
            app_id,
            port,
            handles: Mutex::default(),
            global_handle_monitor,
            connections: ConnectionStatesTable::default(),
            handshakes: HandshakeStateTable::default(),
            ack: PendingAckWindow::new(sender),
            encryption: EncryptionTable::default(),
            fingerprints: FingerprintTable::default(),
            address_session: AddressSessionIdTable::default(),
        }
    }

    /// Joins both layer threads.
    /// **DANGEROUS**: This function blocks the entire async runtime, only use if the protocol is
    /// shutting down, when no other tasks need to be done.
    pub async fn join_layers(&mut self) {
        let handles = o_unwrap_or_return!(lock!(self.handles).take().panic_in_debug(
            "Invariant broken while joining the layer threads: \
            function was called more than once",
        ));

        handles.blocking_join();
    }

    pub fn app_id(&self) -> AppId {
        self.app_id.clone()
    }

    pub fn port(&self) -> Port {
        self.port
    }

    pub async fn session_exists(&self, session_id: SessionId) -> bool {
        lock_read!(self.connections).contains_key(&session_id)
    }

    pub async fn set_handles(&self, transport: JoinHandle<()>, processor: JoinHandle<()>) {
        _ = lock!(self.handles).insert(LayerHandles {
            transport,
            processor,
        });
    }

    pub async fn promote_handshake(
        &self,
        new_session_id: SessionId,
        address: SocketAddr,
        handshake_id: HandshakeId,
        connection: mpsc::Sender<ConnectionEvent>,
        app_id: AppId,
    ) -> Option<(
        EphemeralSecret,
        oneshot::Sender<
            core::result::Result<(SessionId, mpsc::Receiver<ConnectionEvent>), ConnectionError>,
        >,
    )> {
        let Some(HandshakeState {
            ephemeral_secret,
            session_id,
            response,
            ..
        }) = self.handshakes.take(handshake_id).await
        else {
            debug_assert!(
                false,
                "Invariant broken while promoting handshake {handshake_id}: \
                    could not find handshake entry"
            );
            return None;
        };

        let mut lock = lock_write!(self.connections);
        let Entry::Vacant(entry) = lock.entry(new_session_id) else {
            debug_assert!(
                false,
                "Invariant broken while promoting handshake {handshake_id}: \
                    session with session ID {session_id} already exists"
            );
            return None;
        };

        entry.insert(ConnectionStates::Established(Box::new(EstablishedState {
            last_activity: Mutex::default(),
            connection,
            state: SessionStates::Up,
            address: RwLock::new(address),
            app_id,
        })));

        lock_write!(self.address_session)
            .entry(address)
            .and_modify(|v| v.push(session_id))
            .or_insert(vec![session_id]);

        Some((ephemeral_secret, response))
    }

    pub async fn reuse_handshake(
        &self,
        session_id: SessionId,
        handshake_id: HandshakeId,
        ephemeral_secret: EphemeralSecret,
    ) -> Option<HandshakeId> {
        let HandshakeState {
            peer_address,
            session_id: _,
            response,
            ..
        } = lock_write!(self.handshakes).remove(&handshake_id)?;

        let new_handshake_id = HandshakeId::generate().await;

        lock_write!(self.handshakes).insert(
            new_handshake_id,
            HandshakeState {
                peer_address,
                ephemeral_secret,
                session_id,
                response,
            },
        );

        Some(new_handshake_id)
    }

    pub async fn handshake_done(&self, session_id: SessionId) {
        let mut lock = lock_write!(self.connections);
        match lock.remove(&session_id) {
            Some(ConnectionStates::Handshake {
                ack_triggered_response,
                app_id,
                address,
                signal,
            }) => {
                let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);
                lock.insert(
                    session_id,
                    ConnectionStates::Established(Box::new(EstablishedState {
                        last_activity: Mutex::default(),
                        connection: sender,
                        state: SessionStates::Up,
                        address,
                        app_id,
                    })),
                );

                _ = ack_triggered_response.send(Ok((session_id, receiver)));
                signal.send_modify(|m| *m = true);
            }
            Some(established @ ConnectionStates::Established { .. }) => {
                lock.insert(session_id, established);
            }
            None => {
                debug_assert!(
                    false,
                    "Invariant broken while finishing a handshake: \
                        the given session ID {session_id} had no associated sessions"
                );
            }
        }
    }

    pub async fn new_session(
        &self,
        session_id: SessionId,
        response: oneshot::Sender<
            core::result::Result<(SessionId, mpsc::Receiver<ConnectionEvent>), ConnectionError>,
        >,
        address: SocketAddr,
        app_id: AppId,
    ) {
        let mut lock = lock_write!(self.connections);
        let entry = debug_match_or_return!(
            lock.entry(session_id),
            Entry::Vacant(entry) => entry,
            format!(
                "in `new_session`: session {session_id} already existed, this one was with {app_id} from {address:?} "
            )
        );

        entry.insert(ConnectionStates::Handshake {
            ack_triggered_response: response,
            app_id,
            signal: watch::channel(false).0,
            address: RwLock::new(address),
        });

        lock_write!(self.fingerprints).insert(session_id, Arc::new(FingerprintWindow::default()));

        lock_write!(self.address_session)
            .entry(address)
            .and_modify(|v| v.push(session_id))
            .or_insert(vec![session_id]);
    }

    pub async fn advertise_closed(&self) {
        for (session_id, connection) in lock_write!(self.connections).drain() {
            match connection {
                ConnectionStates::Handshake {
                    ack_triggered_response,
                    app_id,
                    address,
                    signal,
                } => {
                    signal.send_modify(|m| *m = false);
                    if ack_triggered_response
                        .send(Err(ConnectionError::ProtocolClosed))
                        .is_err()
                    {
                        debug_assert!(
                            false,
                            "Invariant broken in `advertise_closed`: \
                                response to a handshake request failed. session_id: {}, app_id: {}, address: {}",
                            session_id,
                            app_id,
                            *lock_read!(address)
                        );
                    }
                }
                ConnectionStates::Established(box EstablishedState { connection, .. }) => {
                    if connection
                        .send(ConnectionEvent::ProtocolClosed(vec![]))
                        .await
                        .is_err()
                    {
                        // TODO:
                        // debug_assert!(
                        //    false,
                        //    "Invariant broken in `advertise_closed`: \
                        //        channel to connection with session ID {} with {} ({}) was already closed. state: {}.",
                        //    session_id,
                        //    app_id,
                        //    *lock_read!(address),
                        //    state
                        // );
                    }
                }
            }
        }
    }

    pub async fn send_on_session(
        &self,
        session_id: SessionId,
        buffer: ReadableBuffer,
        sender: watch::Sender<StreamMessage>,
        outbound_sender: ManagerToProcessor,
    ) {
        let event = {
            let mut lock = lock_write!(get_state!().connections);
            let session = o_unwrap_or_return!(lock.get_mut(&session_id).panic_in_debug(&format!(
                "Invariant broken while trying to send on a session \
                with ID {session_id}: session does not exist"
            )));

            session.stream_to(buffer, sender);
            debug_match_or_return!(
                session,
                ConnectionStates::Established(box EstablishedState {
                    state:
                        SessionStates::Streaming(StreamState {
                            streaming: Streaming::To(StreamingTo { event, .. }),
                            ..
                        }), ..
                }) => event,
                format!("Invariant broken while trying to send on a session \
                    with ID {session_id}: session not in correct state even though stream_to() was just called")
            )
            .clone()
        };

        let mut playing = true;
        let mut interval = interval(Duration::from_millis(SEND_TIMEOUT));

        loop {
            select! {
                event = event.listen_then(|m| {
                        if let StreamEvent::Retransmit(v) = m {
                            StreamEvent::Retransmit(std::mem::take(v))
                        } else {
                            m.clone()
                        }
                    }) => {
                        match event {
                            StreamEvent::Pause => playing = false,
                            StreamEvent::Play => playing = true,
                            StreamEvent::Close => {
                                close_outgoing_stream_action(session_id).await;
                                return;
                            }
                            StreamEvent::Retransmit(byte_ranges) if playing => {
                                get_state!()
                                    .global_handle_monitor
                                    .dispatch(retransmit_action(
                                        session_id,
                                        outbound_sender.clone(),
                                        byte_ranges,
                                    )).await;
                            }
                            StreamEvent::Retransmit(_) => {}
                        }
                    }
                _ = interval.tick() => {
                    if playing {
                        get_state!()
                            .global_handle_monitor
                            .dispatch(send_stream_action(
                                session_id,
                                outbound_sender.clone(),
                                PACKET_COUNT_PER_BATCH,
                            )).await;
                    }
                }
            }
        }
    }

    pub async fn close_session(&self, session_id: SessionId) {
        let main_session =
            o_unwrap_or_return!(lock_write!(get_state!().connections).remove(&session_id));

        let address = match main_session {
            ConnectionStates::Handshake {
                ack_triggered_response,
                address,
                ..
            } => {
                _ = ack_triggered_response.send(Err(ConnectionError::SessionClosedByPeer));
                *lock_read!(address)
            }
            ConnectionStates::Established(box EstablishedState { address, .. }) => {
                *lock_read!(address)
            }
        };

        o_unwrap_or_return!(
            get_state!()
                .address_session
                .remove(address, session_id)
                .await
        );

        get_state!().fingerprints.remove_session(session_id);

        lock_write!(get_state!().encryption).remove(&session_id);
    }

    pub async fn close_stream(&self, session_id: SessionId, outbound_sender: ManagerToProcessor) {
        let mut lock = lock_write!(self.connections);
        let session = o_unwrap_or_return!(lock.get_mut(&session_id));

        if let ConnectionStates::Established(box EstablishedState { state, .. }) = session {
            match state {
                SessionStates::Streaming(
                    StreamState {
                        streaming: Streaming::To(StreamingTo { event, .. }),
                        ..
                    },
                    ..,
                ) => {
                    event.update_with(|e| *e = StreamEvent::Close).await;
                }
                SessionStates::Streaming(StreamState { stream, .. }) => {
                    stream.send_modify(|m| m.closed = true);
                }
                _ => {
                    return;
                }
            }

            *state = SessionStates::Up;
        }
    }
}

async fn retransmit_action(
    session_id: SessionId,
    outbound_sender: ManagerToProcessor,
    ranges: Vec<ByteRange>,
) {
    let mut lock = lock_write!(get_state!().connections);
    let session = o_unwrap_or_return!(lock.get_mut(&session_id).panic_in_debug(&format!(
        "Invariant broken while trying to send on a session \
                            with ID {session_id}: session does not exist"
    )));
    if let ConnectionStates::Established(box EstablishedState {
        state:
            SessionStates::Streaming(
                stream @ StreamState {
                    streaming: Streaming::To(_),
                    ..
                },
            ),
        address,
        ..
    }) = session
        && let Some(chunks) = stream.retransmit(ranges)
    {
        send_data_packets(chunks, &outbound_sender, session_id, *lock_read!(address)).await;
    }
}

async fn send_stream_action(session_id: SessionId, outbound_sender: ManagerToProcessor, n: usize) {
    let action = {
        let mut lock = lock_write!(get_state!().connections);
        let session = o_unwrap_or_return!(lock.get_mut(&session_id).panic_in_debug(&format!(
            "Invariant broken while trying to send on a session \
                                with ID {session_id}: session does not exist"
        )));
        if let ConnectionStates::Established(box EstablishedState {
            state:
                SessionStates::Streaming(
                    stream @ StreamState {
                        streaming: Streaming::To(_),
                        ..
                    },
                ),
            address,
            ..
        }) = session
            && let Some(chunks) = stream.get_chunks(n)
        {
            let addr = *lock_read!(address);
            let close_event =
                if let Streaming::To(StreamingTo { buffer, event }) = &stream.streaming {
                    buffer.is_done().then(|| event.clone())
                } else {
                    None
                };
            Some((chunks, addr, close_event))
        } else {
            None
        }
    };

    if let Some((chunks, addr, close_event)) = action {
        send_data_packets(chunks, &outbound_sender, session_id, addr).await;
        if let Some(event) = close_event {
            event.update(StreamEvent::Close).await;
        }
    }
}

async fn close_outgoing_stream_action(session_id: SessionId) {
    let mut lock = lock_write!(get_state!().connections);
    let session = o_unwrap_or_return!(lock.get_mut(&session_id).panic_in_debug(&format!(
        "Invariant broken while trying to send on a session \
                            with ID {session_id}: session does not exist"
    )));

    if let ConnectionStates::Established(box EstablishedState { state, .. }) = session {
        if let SessionStates::Streaming(stream_state) = state {
            stream_state.close();
        }
        *state = SessionStates::Up;
    }
}

async fn send_data_packets(
    chunks: Vec<(BytePosition, Box<[u8]>)>,
    outbound_sender: &ManagerToProcessor,
    session_id: SessionId,
    address: SocketAddr,
) {
    for (position, payload) in chunks {
        let packet = DataPacket::new(
            Options::none(),
            BatchID::new(1),
            inference::TEMP_FEC,
            session_id,
            position,
            payload,
        );
        get_state!()
            .global_handle_monitor
            .dispatch(packet.send(outbound_sender.clone(), address))
            .await;
    }
}

impl AddressSessionIdTable {
    pub async fn free_session(&self, address: SocketAddr) -> Option<SessionId> {
        let lock = lock_read!(self);
        let connections = lock_read!(get_state!().connections);
        lock.get(&address)?
            .iter()
            .find(|session| {
                matches!(
                    connections.get(session),
                    Some(ConnectionStates::Established(box EstablishedState {
                        state: SessionStates::Up | SessionStates::Down,
                        ..
                    }))
                )
            })
            .copied()
    }

    pub async fn remove(&self, address: SocketAddr, session_id: SessionId) -> Option<SessionId> {
        let mut lock = lock_write!(self);
        let v = lock.get_mut(&address)?;
        let index = v.iter().position(|e| e.eq(&session_id))?;
        v.swap_remove(index);
        if v.is_empty() {
            lock.remove(&address);
        }

        Some(session_id)
    }
}

pub struct GeneralSessionState {
    last_key_rotation_time: Timestamp,
    flags: SessionStateFlags,
}

impl GeneralStateTable {
    pub async fn last_key_rotation_time(&self, session_id: SessionId) -> Option<Timestamp> {
        Some(lock_read!(self).get(&session_id)?.last_key_rotation_time)
    }

    pub async fn key_rotation(&self, session_id: SessionId) -> Option<()> {
        lock_write!(self)
            .get_mut(&session_id)?
            .last_key_rotation_time = Timestamp::now();
        Some(())
    }

    pub async fn flags(&self, session_id: SessionId) -> Option<SessionStateFlags> {
        Some(lock_read!(self).get(&session_id)?.flags)
    }

    pub async fn flags_then(
        &self,
        session_id: SessionId,
        flag: <SessionStateFlags as Flags>::FlagType,
        mut f: impl FnMut(
            SessionStateFlags,
            <SessionStateFlags as Flags>::FlagType,
        ) -> SessionStateFlags,
    ) -> Option<()> {
        let mut lock = lock_write!(self);
        let flags = lock.get(&session_id)?.flags;
        let new = f(flags, flag);
        lock.get_mut(&session_id)?.flags = new;
        Some(())
    }
}

impl FingerprintTable {
    pub async fn add_session(&self, session_id: SessionId) {
        lock_write!(self.0).insert(session_id, Arc::default());
    }

    pub async fn remove_session(&self, session_id: SessionId) {
        lock_write!(self.0).remove(&session_id);
    }
}

impl LastActivityTable {
    pub fn update(&mut self, session_id: SessionId, ts: Timestamp) {
        self.0
            .entry(session_id)
            .and_modify(|v| v.update(ts.get()))
            .or_insert(ForeignTimestamp::new(ts.get()));
    }

    pub fn read(&self, session_id: SessionId) -> Option<Timestamp> {
        self.0.get(&session_id).map(Timestamp::from)
    }
}

#[derive(Flags, Clone, Copy)]
#[repr(transparent)]
#[flagtype(AppOptionFlag)]
pub struct AppOptions(u32);

#[derive(Clone, Copy)]
#[repr(u32)]
#[variants_array]
pub enum AppOptionFlag {
    ApproveAllApps = 1 << 0,
}

struct LayerHandles {
    transport: JoinHandle<()>,
    processor: JoinHandle<()>,
}

impl LayerHandles {
    fn new(transport: JoinHandle<()>, processor: JoinHandle<()>) -> Self {
        Self {
            transport,
            processor,
        }
    }

    /// Joins both layers
    /// **DANGEROUS**: This function blocks the entire async runtime, only use if the protocol is
    /// shutting down, when no other tasks need to be done.
    fn blocking_join(self) {
        _ = self.transport.join();
        _ = self.processor.join();
    }
}

#[derive(Debug, Serialize, Deref, Clone, Copy)]
#[repr(transparent)]
pub struct Port(u16);

impl Port {
    #[must_use]
    pub fn new(port: u16) -> Self {
        Port(port)
    }
}

impl From<SocketAddr> for Port {
    fn from(value: SocketAddr) -> Self {
        match value {
            SocketAddr::V4(socket_addr_v4) => Port(socket_addr_v4.port()),
            SocketAddr::V6(socket_addr_v6) => Port(socket_addr_v6.port()),
        }
    }
}

impl From<SocketAddrV4> for Port {
    fn from(value: SocketAddrV4) -> Self {
        Port(value.port())
    }
}

impl From<SocketAddrV6> for Port {
    fn from(value: SocketAddrV6) -> Self {
        Port(value.port())
    }
}

#[derive(PartialEq, Clone, Copy)]
#[repr(u32)]
#[variants_array]
pub enum SessionStateFlag {
    Handshake = 1 << 1,
    CurrentlyStreamingFrom = 1 << 5,
    CurrentlyStreamingTo = 1 << 6,
}

#[derive(Flags, Serialize, Debug, PartialEq, Clone, Copy)]
#[repr(transparent)]
#[flagtype(SessionStateFlag)]
pub struct SessionStateFlags(u32);

#[derive(Display, Hash, Eq, PartialEq, Debug, Clone, Copy, Serialize)]
#[repr(transparent)]
pub struct HandshakeId(u32);

impl HandshakeId {
    pub async fn generate() -> Self {
        let lock = lock_read!(get_state!().handshakes);
        loop {
            let r = Self(rand::random::<u32>());
            if !lock.contains_key(&r) && r.0 != 0 {
                return r;
            }
        }
    }
}

pub struct HandshakeState {
    pub peer_address: SocketAddr,
    pub ephemeral_secret: EphemeralSecret,
    pub session_id: SessionId,
    pub response: oneshot::Sender<
        core::result::Result<(SessionId, mpsc::Receiver<ConnectionEvent>), ConnectionError>,
    >,
}

impl HandshakeState {
    #[must_use]
    pub fn new(
        peer_address: SocketAddr,
        ephemeral_secret: EphemeralSecret,
        session_id: SessionId,
        response: oneshot::Sender<
            core::result::Result<(SessionId, mpsc::Receiver<ConnectionEvent>), ConnectionError>,
        >,
    ) -> Self {
        Self {
            peer_address,
            ephemeral_secret,
            session_id,
            response,
        }
    }
}

#[derive(Deref, Debug, Clone, Display)]
#[repr(transparent)]
pub struct AppId(String);

impl AppId {
    // Large enough without exceeding the mac packet size with the number of headers on the
    // HelloPacket
    pub const MAX_LENGTH: usize = 512;
    #[must_use]
    pub fn new(id: String) -> Self {
        debug_assert!(
            id.is_ascii(),
            "Invariant broken while constructing `AppId`: \
            The ID is not a valid ascii sequence: {id}"
        );

        debug_assert!(
            id.len() < Self::MAX_LENGTH,
            "Invariant broken while constructing `AppId`: \
                The ID is larger than `Self::MAX_LENGTH` ({} >= {})",
            id.len(),
            Self::MAX_LENGTH
        );

        Self(id)
    }
}

impl From<&str> for AppId {
    fn from(value: &str) -> Self {
        debug_assert!(
            value.is_ascii(),
            "Invariant broken while constructing `AppId`: \
            The ID is not a valid ascii sequence: {value}"
        );

        debug_assert!(
            value.len() < Self::MAX_LENGTH,
            "Invariant broken while constructing `AppId`: \
                The ID is larger than `Self::MAX_LENGTH` ({} >= {})",
            value.len(),
            Self::MAX_LENGTH
        );

        Self(String::from(value))
    }
}

impl Serialize for AppId {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.0.len() {
            Err(())
        } else {
            buf.copy_from_slice(self.0.as_bytes());
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        let id = String::from_utf8(Vec::from(bytes)).map_err(|_| ())?;
        if id.is_ascii() {
            Ok(AppId::new(id))
        } else {
            Err(())
        }
    }

    fn sized(&self) -> usize {
        self.0.len()
    }
}

impl HandshakeStateTable {
    pub async fn new_handshake(
        &self,
        handshake_id: HandshakeId,
        peer_address: SocketAddr,
        ephemeral_secret: EphemeralSecret,
        session_id: SessionId,
        response: oneshot::Sender<
            core::result::Result<(SessionId, mpsc::Receiver<ConnectionEvent>), ConnectionError>,
        >,
    ) {
        lock_write!(self).insert(
            handshake_id,
            HandshakeState::new(peer_address, ephemeral_secret, session_id, response),
        );
    }

    pub async fn take(&self, id: HandshakeId) -> Option<HandshakeState> {
        lock_write!(self).remove(&id)
    }
}

impl SessionAddressTable {
    pub async fn contains(&self, id: SessionId) -> bool {
        self.read().await.contains_key(&id)
    }

    pub async fn address_changed(&self, id: SessionId, address: SocketAddr) -> bool {
        !matches!(lock_read!(self).get(&id), Some(addr) if *addr == address)
    }

    pub async fn update(&self, id: SessionId, address: SocketAddr) -> SocketAddr {
        let mut lock = lock_write!(self);
        let addr = lock.entry(id).or_insert(address);
        addr.set_ip(address.ip());
        *addr
    }
}

#[derive(Default, Debug)]
pub struct SessionFecState {
    table: RwLock<HashMap<BatchID, FecBatchWindow>>,
}

impl SessionFecState {
    pub async fn add_data(
        &self,
        batch_id: BatchID,
        FECInfo {
            batch_size,
            batch_pos,
            recovery_count,
        }: FECInfo,
    ) {
        let mut lock = lock_write!(self.table);
        lock.entry(batch_id)
            .or_insert(FecBatchWindow::new(batch_size, recovery_count))
            .add_data(batch_pos as usize);
    }

    pub async fn add_parity(
        &self,
        batch_id: BatchID,
        FECInfo {
            batch_size,
            recovery_count,
            ..
        }: FECInfo,
    ) {
        let mut lock = lock_write!(self.table);
        lock.entry(batch_id)
            .or_insert(FecBatchWindow::new(batch_size, recovery_count))
            .add_parity();
    }
}

#[derive(Debug)]
struct FecBatchWindow {
    batch_size: u8,
    recovery_count: u8,
    data_arrived: FecArrivedBitMap,
    recovery_arrived: FecArrivedBitMap,
}

impl FecBatchWindow {
    fn new(batch_size: u8, recovery_count: u8) -> Self {
        Self {
            batch_size,
            recovery_count,
            data_arrived: FecArrivedBitMap::default(),
            recovery_arrived: FecArrivedBitMap::default(),
        }
    }

    fn recovery_ready(&self) -> bool {
        self.data_arrived.count_set() + self.recovery_arrived.count_set() >= self.batch_size
    }

    #[inline]
    fn add_data(&mut self, index: usize) {
        self.data_arrived.set_bit(index);
    }

    #[inline]
    #[cfg(feature = "fec_xor")]
    fn add_parity(&mut self) {
        self.recovery_arrived.set_bit(0);
    }

    #[inline]
    #[cfg(all(feature = "fec_rs", not(feature = "fec_xor")))]
    fn add_parity(&mut self, index: usize) {
        self.recovery_arrived.set_bit(index);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FecArrivedBitMap([u128; 2]);

impl FecArrivedBitMap {
    /// Sets the bit of the given index
    #[inline]
    fn set_bit(&mut self, index: usize) {
        self.0[index / 128] |= 1 << (index % 128);
    }

    /// Returns true if enough bits are set based on a specified threshold
    #[inline]
    fn enough_set(&self, threshold: u8) -> bool {
        self.0[0].count_ones() + self.0[1].count_ones() >= threshold as u32
    }

    #[allow(clippy::cast_possible_truncation)]
    fn count_set(&self) -> u8 {
        (self.0[0].count_ones() + self.0[1].count_ones()) as u8
    }

    /// Returns true if the bit under the specified index is set
    #[inline]
    fn is_set(&self, index: usize) -> bool {
        (self.0[index / 128] >> (index % 128)) % 2 == 1
    }
}

#[derive(Clone, Copy)]
pub struct PendingAckMonitor {
    table: &'static PendingAckWindow,
}

impl PendingAckMonitor {
    pub fn new(table: &'static PendingAckWindow) -> Self {
        Self { table }
    }

    pub async fn add(&self, packet: Packet) {
        self.table.add(packet).await;
    }
}

#[derive(Eq, Deref)]
struct FingerprintPtr(*const PacketFingerprint);

unsafe impl Send for FingerprintPtr {}
unsafe impl Sync for FingerprintPtr {}

impl FingerprintPtr {
    fn from_ref(value: &PacketFingerprint) -> Self {
        Self(std::ptr::from_ref(value))
    }
}

impl PartialEq for FingerprintPtr {
    fn eq(&self, other: &Self) -> bool {
        unsafe { (*self.0) == (*other.0) }
    }
}

impl core::hash::Hash for FingerprintPtr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            (*self.0).hash(state);
        }
    }
}

#[derive(Clone, Copy)]
pub struct FingerprintMonitor {
    table: &'static FingerprintTable,
}

impl FingerprintMonitor {
    pub fn new(table: &'static FingerprintTable) -> Self {
        Self { table }
    }

    pub async fn add(&self, session_id: SessionId) {
        let mut table = self.table.write().await;
        table.insert(session_id, Arc::default());
    }

    /// returns an Arc to the window for this session
    ///
    /// # Panics
    /// This function panics if the session is not yet initialized - an Invariant
    pub async fn get(&self, session_id: &SessionId) -> Arc<FingerprintWindow> {
        let table = self.table.read().await;
        let Some(window) = table.get(session_id) else {
            panic!(
                "Invariant broken while trying to get a `FingerprintWindow`:\
            {session_id} is not a valid session"
            );
        };

        window.clone()
    }
}

struct PendingAckQueueEntry {
    timestamp: Timestamp,
    ptr: FingerprintPtr,
    retries: u8,
}

impl PendingAckQueueEntry {
    const MAX_RETRIES: u8 = 5;
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;

    fn new(ptr: &PacketFingerprint) -> Self {
        Self {
            timestamp: Timestamp::now(),
            ptr: FingerprintPtr::from_ref(ptr),
            retries: 0,
        }
    }

    fn retried(&mut self) -> bool {
        self.retries += 1;
        self.retries > Self::MAX_RETRIES
    }

    #[inline]
    fn ready_to_retry(&self) -> bool {
        self.timestamp.been_longer_than(Self::PRUNE_INTERVAL)
    }
}

pub struct PendingAckWindow {
    pending: RwLock<HashMap<PacketFingerprint, Packet>>,
    queue: Mutex<VecDeque<PendingAckQueueEntry>>,
    sender: ManagerToProcessor,
    canceled: AtomicBool,
}

impl PendingAckWindow {
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;
    const BUFFERING_TIME: u64 = 2 * 1000;

    #[must_use]
    pub fn new(sender: ManagerToProcessor) -> Self {
        Self {
            pending: RwLock::default(),
            queue: Mutex::default(),
            sender,
            canceled: AtomicBool::new(false),
        }
    }

    pub async fn init(self: Arc<Self>) {
        get_state!()
            .global_handle_monitor
            .dispatch(self.prune())
            .await;
    }

    pub async fn add(&'static self, packet: Packet) {
        get_state!()
            .global_handle_monitor
            .dispatch(self.inner_add(packet))
            .await;
    }

    pub async fn acknowledge(&self, fingerprint: impl Into<PacketFingerprint>) {
        let fingerprint = fingerprint.into();
        lock_write!(self.pending).remove(&fingerprint);
    }

    async fn inner_add(&self, packet: Packet) {
        let fingerprint = r_unwrap_or_return!(PacketFingerprint::try_from(&packet).panic_in_debug(
            &format!(
                "Invariant broken while adding a packet to `PendingAckWindow`:\
                A packet that should not be acked was provided ({packet:?}) full list can\
                be found at the impl TryFrom<&Packet> for PacketFingerprint",
            )
        ));

        let entry = PendingAckQueueEntry::new(&fingerprint);
        lock_write!(self.pending).insert(fingerprint, packet);
        lock!(self.queue).push_back(entry);
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired = Vec::with_capacity(256);
        let mut to_retry = Vec::with_capacity(256);

        while !self.canceled.load(Ordering::Relaxed) {
            let top_timestamp = {
                // get expired pending ack packets as well as ones to retry
                let mut queue = lock!(self.queue);
                while queue
                    .front()
                    .is_some_and(PendingAckQueueEntry::ready_to_retry)
                {
                    if let Some(mut value) = queue.pop_front() {
                        if value.retried() {
                            expired.push(value.ptr);
                        } else {
                            value.timestamp.set_again();
                            to_retry.push(value);
                        }
                    }
                }

                // return the time until next pending ack needs a retry
                match queue.front() {
                    Some(top) => Timestamp::now().get() - top.timestamp.get(),
                    None => Self::PRUNE_INTERVAL - Self::BUFFERING_TIME,
                }
            };

            // resend pending acks
            {
                let pending = lock_read!(self.pending);
                let mut queue = lock!(self.queue);
                let lock = lock_read!(get_state!().connections);
                for entry in to_retry.drain(..) {
                    let Some(packet) = pending.get(unsafe { &**entry.ptr }) else {
                        continue;
                    };

                    let Some(ConnectionStates::Established(box EstablishedState {
                        address, ..
                    })) = lock.get(o_unwrap_or_return!(&packet.session_id().panic_in_debug(
                        &format!(
                            "A packet that should never be acked has been inserted: {packet:?}"
                        )
                    )))
                    else {
                        continue;
                    };

                    queue.push_back(entry);

                    Box::new(packet.clone())
                        .send(self.sender.clone(), *lock_read!(address))
                        .await;
                }
            }

            // remove expired acks
            {
                let mut pending = lock_write!(self.pending);
                expired
                    .drain(..)
                    .for_each(|ptr| _ = pending.remove(unsafe { &**ptr }));
            }

            tokio::time::sleep(Duration::from_millis(top_timestamp + Self::BUFFERING_TIME)).await;
        }
    }
}

pub struct FingerprintWindow {
    fingerprints: RwLock<HashSet<Box<PacketFingerprint>>>,
    queue: Mutex<VecDeque<(Timestamp, FingerprintPtr)>>,
    canceled: AtomicBool,
}

impl Default for FingerprintWindow {
    fn default() -> Self {
        Self {
            fingerprints: RwLock::new(HashSet::new()),
            queue: Mutex::new(VecDeque::new()),
            canceled: AtomicBool::new(false),
        }
    }
}

impl FingerprintWindow {
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;
    const BUFFERING_TIME: u64 = 2 * 1000;

    pub async fn init(self: Arc<Self>) {
        get_state!()
            .global_handle_monitor
            .dispatch(self.prune())
            .await;
    }

    #[must_use]
    pub async fn contains(&self, fingerprint: &PacketFingerprint) -> bool {
        let fingerprints = self.fingerprints.read().await;
        fingerprints.contains(fingerprint)
    }

    pub async fn add(&self, fingerprint: Box<PacketFingerprint>) -> bool {
        let ptr = {
            let mut fingerprints = lock_write!(self.fingerprints);
            let ptr = FingerprintPtr::from_ref(&fingerprint);
            if !fingerprints.insert(fingerprint) {
                return false;
            }

            ptr
        };

        let mut queue = self.queue.lock().await;
        queue.push_back((Timestamp::now(), ptr));

        true
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired = Vec::with_capacity(256);
        while !self.canceled.load(Ordering::Relaxed) {
            let top_timestamp = {
                let mut queue = self.queue.lock().await;
                while queue
                    .front()
                    .is_some_and(|(ts, _)| ts.been_longer_than(Self::PRUNE_INTERVAL))
                {
                    if let Some((_, ptr)) = queue.pop_front() {
                        expired.push(ptr);
                    }
                }
                match queue.front() {
                    Some(top) => Timestamp::now().get() - top.0.get(),
                    None => Self::PRUNE_INTERVAL - Self::BUFFERING_TIME,
                }
            };

            {
                let mut fingerprints = self.fingerprints.write().await;
                expired
                    .drain(..)
                    .for_each(|ptr| _ = fingerprints.remove(unsafe { &**ptr }));
            }

            tokio::time::sleep(Duration::from_millis(top_timestamp + Self::BUFFERING_TIME)).await;
        }
    }
}

pub struct EncryptionWindow {
    cipher: Arc<Aes256GcmSiv>,
    nonce: AtomicU64,
}

impl EncryptionWindow {
    #[must_use]
    pub fn new(cipher: Aes256GcmSiv) -> Self {
        Self {
            cipher: Arc::new(cipher),
            nonce: AtomicU64::new(0),
        }
    }

    pub fn get(&self) -> (Arc<Aes256GcmSiv>, [u8; 8]) {
        let x = self
            .nonce
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (self.cipher.clone(), x.to_be_bytes())
    }

    pub fn get_cipher(&self) -> Arc<Aes256GcmSiv> {
        self.cipher.clone()
    }
}

#[derive(Clone, Copy)]
pub struct EncryptionMonitor {
    table: &'static EncryptionTable,
}

impl EncryptionMonitor {
    pub fn new(table: &'static EncryptionTable) -> Self {
        Self { table }
    }

    /// returns the key and nonce counter, increasing it in the process, for a specific session
    ///
    /// # Panics
    /// This function panics if the key is not yet created, which should be impossible
    pub async fn get(&self, session_id: &SessionId) -> Option<(Arc<Aes256GcmSiv>, [u8; 8])> {
        Some(self.table.write().await.get(session_id)?.get())
    }

    /// returns the key without increasing the counter
    ///
    /// # Panics
    /// This function panics if the key is not yet created, which should be impossible
    pub async fn get_cipher(&self, session_id: &SessionId) -> Option<Arc<Aes256GcmSiv>> {
        Some(self.table.read().await.get(session_id)?.get_cipher())
    }
}
