use aes_gcm_siv::Aes256GcmSiv;
use derive_more::{Deref, Display};
use tokio::{
    select,
    sync::{Mutex, RwLock, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{interval, sleep as tokio_sleep},
};
use tracing::{debug, error, instrument};
use x25519_dalek::EphemeralSecret;

use crate::{
    api::{ReadableBuffer, WriteableBuffer},
    debug_match_or_return, get_state, lock, lock_read, lock_write,
    manager::{
        CHANNEL_BUFFER_SIZE, STATE,
        packets::{
            BatchID, BytePosition, ByteRange, DataPacket, FECInfo, FecConfig, KeepAlivePacket,
            MAX_PAYLOAD_LENGTH, OptionFlags, Options, Packet, PacketFingerprint,
            PlaybackControlPacket, RetransmitPacket, SessionId,
        },
        types::{ForeignTimestamp, ManagerToProcessor},
    },
    o_unwrap_or_return,
    packet_processor::fec::{self, Recovered},
    prelude::*,
    r_unwrap_or_return,
};
use core::panic;
use std::{
    collections::{BTreeSet, HashSet, VecDeque, hash_map::Entry},
    convert::identity,
    fmt::Display,
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
    },
    time::Duration,
};

const PACKET_DISCARD_TIME_MS: u64 = 500;
const SEND_INTERVAL: Duration = Duration::from_millis(25);
pub const PACKET_COUNT_PER_BATCH: usize = 28;
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_millis(500);

macro_rules! sessions_state_fields {
    ($($name:ident($key:ty => $value:ty)),*) => {
        $(
            #[derive(Default, Deref)]
            pub struct $name(RwLock<HashMap<$key, $value>>);
        )*
    };
}

sessions_state_fields!(
    EncryptionTable(SessionId => EncryptionWindow),
    SessionAddressTable(SessionId => SocketAddr),
    HandshakeStateTable(HandshakeId => HandshakeState),
    AddressSessionIdTable(SocketAddr => Vec<SessionId>)
);

#[derive(Default)]
pub struct LastActivityTable(HashMap<SessionId, ForeignTimestamp>);

#[derive(Default, Debug, Deref)]
pub struct ConnectionStatesTable(RwLock<HashMap<SessionId, ConnectionStates>>);

#[derive(Debug)]
pub struct StreamingTo {
    pub buffer: ReadableBuffer,
    pub current_batch: AtomicU16,
    pub event: Arc<Shared<StreamEvent>>,
    /// FEC config the app chose at stream-open time. Read by
    /// [`send_data_packets`] to build the `FECInfo` on every outbound
    /// data packet. Not consulted by the FEC module — the receiver
    /// learns the scheme from the wire byte in `FECInfo.scheme`.
    pub fec_config: FecConfig,
}

impl StreamingTo {
    /// Atomically increment the batch counter and return the new `BatchID`.
    pub fn next_batch_id(&self) -> BatchID {
        // Start state is 0; the first call returns 1, satisfying
        // `BatchID::new`'s nonzero invariant. Wrapping back to 0 after
        // u16::MAX batches will hit that invariant — currently out of scope.
        let raw = self
            .current_batch
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        BatchID::new(raw)
    }
}

#[derive(Debug)]
pub struct StreamingFrom {
    pub buffer: WriteableBuffer,
    pub pending: HashMap<usize, Timestamp>,
    /// Session this `StreamingFrom` belongs to. Stored only so the
    /// retransmit-request emission path (site 5) can tag its observations
    /// without threading `session_id` through three function signatures.
    session_id: SessionId,
}

impl StreamingFrom {
    /// How long a pending retransmit request stays in the dedup map before
    /// being assumed lost and cleared, freeing the chunk to be re-requested.
    pub const PENDING_TTL_MS: u64 = PACKET_DISCARD_TIME_MS;

    #[must_use]
    pub fn new(buffer: WriteableBuffer, session_id: SessionId) -> Self {
        Self {
            buffer,
            pending: HashMap::default(),
            session_id,
        }
    }

    /// Filter `positions` down to those that should actually be requested:
    /// drop chunks already filled and chunks already pending. Mark the
    /// accepted ones as pending so subsequent calls (from the same or another
    /// decider) skip them. Sweeps stale pending markers first so a lost
    /// response from a previous round doesn't permanently block a chunk.
    ///
    /// Site 5: emits `HolesObserved` (count of *new* holes — positions that
    /// weren't already pending and aren't filled), `RetransmitIssued` (the
    /// post-filter accepted count, same as the returned `Vec` length), and
    /// one `LossBurst` per terminated run of consecutive accepted chunks.
    #[allow(clippy::cast_possible_truncation)]
    pub fn reserve_for_request(&mut self, positions: Vec<BytePosition>) -> Vec<BytePosition> {
        self.pending
            .retain(|_, ts| !ts.been_longer_than(Self::PENDING_TTL_MS));
        let now = Timestamp::now();
        let session = self.session_id;
        let dc = get_state!().data_collection.clone();

        let mut accepted: Vec<BytePosition> = Vec::with_capacity(positions.len());
        let mut new_holes: u32 = 0;
        let mut current_burst: u32 = 0;
        let mut last_idx: Option<usize> = None;

        for pos in positions {
            let idx = (*pos as usize) / MAX_PAYLOAD_LENGTH;
            if self.buffer.position_occupied(pos).unwrap_or(true) {
                continue;
            }
            // A fresh hole this caller can't yet have seen pending on.
            new_holes = new_holes.saturating_add(1);
            if self.pending.contains_key(&idx) {
                // Don't re-issue, but the burst run still extends across
                // consecutive missing chunks regardless of dedup state.
                last_idx = Some(idx);
                continue;
            }
            self.pending.insert(idx, now);
            // Extend or restart the contiguous-run counter.
            if last_idx.is_some_and(|prev| prev + 1 == idx) {
                current_burst = current_burst.saturating_add(1);
            } else {
                if current_burst > 0 {
                    dc.post(Observation::LossBurst {
                        session,
                        length: current_burst,
                    });
                }
                current_burst = 1;
            }
            last_idx = Some(idx);
            accepted.push(pos);
        }
        if current_burst > 0 {
            dc.post(Observation::LossBurst {
                session,
                length: current_burst,
            });
        }
        if new_holes > 0 {
            dc.post(Observation::HolesObserved {
                session,
                count: new_holes,
            });
        }
        if let Ok(issued) = u32::try_from(accepted.len())
            && issued > 0
        {
            dc.post(Observation::RetransmitIssued {
                session,
                count: issued,
            });
        }

        accepted
    }

    /// Clear the pending marker for a chunk that just arrived. No-op if not
    /// pending.
    ///
    /// Site 6: if a pending marker was present, the chunk was previously
    /// requested via retransmit. Subtract the local-clock stamp from `now`
    /// to derive an RTT sample and post it. Local clock both ends — see
    /// memory `project_per_peer_epoch.md`.
    pub fn clear_pending(&mut self, position: BytePosition) {
        let idx = (*position as usize) / MAX_PAYLOAD_LENGTH;
        if let Some(stamped) = self.pending.remove(&idx) {
            let now = Timestamp::now();
            let rtt = now.get().saturating_sub(stamped.get());
            let rtt_ms = u32::try_from(rtt).unwrap_or(u32::MAX);
            get_state!()
                .data_collection
                .post(Observation::RetransmitRtt {
                    session: self.session_id,
                    rtt_ms,
                });
        }
    }

    /// Run the chunk-level score policy: pick the highest-scoring holes that
    /// clear the threshold (`WriteableBuffer::requestable_areas`), drop any
    /// chunk whose byte position falls inside an active FEC batch (still
    /// expected via the primary path), then filter through the pending dedup.
    /// Returns ready-to-send `ByteRange`s; empty means nothing to request.
    #[allow(clippy::cast_possible_truncation)]
    pub fn score_policy_pick(&mut self, fec_active: &[Range<usize>]) -> Vec<ByteRange> {
        let positions: Vec<BytePosition> = self
            .buffer
            .requestable_areas()
            .iter()
            .flat_map(|area| {
                let r = area.range();
                (r.start..r.end).map(|i| BytePosition((i * MAX_PAYLOAD_LENGTH) as u32))
            })
            .filter(|pos| {
                let p = **pos as usize;
                !fec_active.iter().any(|r| r.contains(&p))
            })
            .collect();
        let accepted = self.reserve_for_request(positions);
        coalesce_byte_positions(accepted)
    }

    /// End-of-stream variant of `score_policy_pick`: take **every** invalid
    /// area regardless of score, filter through FEC-active and pending
    /// dedup. Once `head` reaches `len`, every remaining hole is confirmed
    /// loss; the score-policy threshold no longer makes sense and would
    /// orphan small isolated holes.
    #[allow(clippy::cast_possible_truncation)]
    pub fn finalize_sweep_pick(&mut self, fec_active: &[Range<usize>]) -> Vec<ByteRange> {
        let positions: Vec<BytePosition> = self
            .buffer
            .all_invalid_areas()
            .iter()
            .flat_map(|area| {
                let r = area.range();
                (r.start..r.end).map(|i| BytePosition((i * MAX_PAYLOAD_LENGTH) as u32))
            })
            .filter(|pos| {
                let p = **pos as usize;
                !fec_active.iter().any(|r| r.contains(&p))
            })
            .collect();
        let accepted = self.reserve_for_request(positions);
        coalesce_byte_positions(accepted)
    }
}

#[derive(Debug)]
pub enum Streaming {
    To(StreamingTo),
    From(StreamingFrom),
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

    /// Read the payload for each requested range. Each `ByteRange` here is
    /// expected to be chunk-sized (length ≤ `MAX_PAYLOAD_LENGTH`); the queue
    /// upstream splits multi-chunk requests via `split_range_into_chunks`.
    /// Oversized ranges are clamped to one chunk for safety.
    #[allow(clippy::cast_possible_truncation)]
    pub fn retransmit(&mut self, ranges: Vec<ByteRange>) -> Option<Vec<(BytePosition, Box<[u8]>)>> {
        if let Streaming::To(StreamingTo { buffer, .. }) = &mut self.streaming
            && !self.stream.borrow().paused
            && !self.stream.borrow().closed
        {
            let mut buf = vec![];
            for range in ranges {
                let start = *range.start as usize;
                let len = (range.length as usize).min(MAX_PAYLOAD_LENGTH);
                let end = (start + len).min(buffer.len());
                if let Some(payload) = buffer.read(start..end) {
                    buf.push((range.start, Box::from(payload)));
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
    pub session_id: SessionId,
    pub last_activity: Mutex<ForeignTimestamp>,
    pub connection: mpsc::Sender<InnerConnectionEvent>,
    pub state: SessionStates,
    pub address: RwLock<SocketAddr>,
    pub app_id: AppId,
}

#[derive(Debug)]
pub enum ConnectionStates {
    Handshake {
        session_id: SessionId,
        ack_triggered_response: oneshot::Sender<
            core::result::Result<
                (SessionId, mpsc::Receiver<InnerConnectionEvent>),
                ConnectionError,
            >,
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
        session_id: SessionId,
    ) {
        match self {
            Self::Established(established) => {
                if let EstablishedState {
                    state: state @ (SessionStates::Up | SessionStates::Down),
                    ..
                } = established.as_mut()
                {
                    *state = SessionStates::Streaming(StreamState {
                        streaming: Streaming::From(StreamingFrom::new(buffer, session_id)),
                        stream: sender,
                        fec: SessionFecState::default(),
                    });
                } else {
                    let EstablishedState {
                        state,
                        address,
                        app_id,
                        ..
                    } = established.as_ref();
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
    pub fn stream_to(
        &mut self,
        buffer: ReadableBuffer,
        sender: watch::Sender<StreamMessage>,
        fec_config: FecConfig,
    ) {
        match self {
            Self::Established(established) => {
                if let EstablishedState {
                    state: state @ (SessionStates::Up | SessionStates::Down),
                    ..
                } = established.as_mut()
                {
                    *state = SessionStates::Streaming(StreamState {
                        streaming: Streaming::To(StreamingTo {
                            buffer,
                            event: Arc::default(),
                            // First `next_batch_id()` call yields 1.
                            current_batch: AtomicU16::new(0),
                            fec_config,
                        }),
                        stream: sender,
                        fec: SessionFecState::default(),
                    });
                } else {
                    let EstablishedState {
                        state,
                        address,
                        app_id,
                        ..
                    } = established.as_ref();
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
        if let ConnectionStates::Established(established) = self
            && let EstablishedState {
                state: state @ SessionStates::Streaming(_),
                ..
            } = established.as_mut()
        {
            let SessionStates::Streaming(stream_state) = state else {
                unreachable!("Any other arm has been handled by the let else statement above");
            };

            if let Streaming::To(StreamingTo { event, .. }) = &stream_state.streaming {
                event
                    .update(StreamEvent::Playback(PlaybackControl::Close))
                    .await;
            }

            stream_state.stream.send_modify(|m| m.closed = true);

            *state = SessionStates::Up;
        } else {
            debug_assert!(
                false,
                "Invariant broken in `close_stream`: function has been called on a session with no open stream"
            );
        }
    }

    // TODO:
    /// # Errors
    pub async fn received_data_packet(
        &mut self,
        packet: DataPacket,
        sender: ManagerToProcessor,
    ) -> Result<bool> {
        let address_copy: SocketAddr;
        let declare_done: bool;
        let score_ranges: Vec<ByteRange>;
        if let ConnectionStates::Established(established) = self
            && let EstablishedState {
                state:
                    SessionStates::Streaming(StreamState {
                        streaming: Streaming::From(streaming_from),
                        stream,
                        fec,
                    }),
                address,
                ..
            } = established.as_mut()
        {
            debug!("received data: {}", packet.byte_range_start);
            fec.add_data(packet.batch_id, packet.fec_info, packet.byte_range_start);

            let payload = packet.payload.clone().take();
            match streaming_from
                .buffer
                .write(packet.byte_range_start, payload)
            {
                Ok(_) => {}
                Err(e @ BufferError::FailedToDeref) => {
                    error!("FATAL: {e}");
                    return Err(Error::FailedToDeref);
                }
                Err(_) => {
                    return Err(Error::IrrelevantError);
                }
            }

            // Data arrived for this chunk — drop any pending retransmit marker.
            streaming_from.clear_pending(packet.byte_range_start);

            stream.send_modify(|m| {
                m.head = streaming_from.buffer.head();
                m.approved.replace(true);
            });

            // Site 3 (buffer state): emit current head/len snapshot. Fires
            // once per received data packet; the collector keeps the latest.
            get_state!().data_collection.post(Observation::BufferState {
                session: packet.session_id,
                head: streaming_from.buffer.head() as u64,
                len: streaming_from.buffer.len() as u64,
            });

            let complete_allow_partial = stream.borrow().complete.is_some_and(identity);
            let head_at_end = streaming_from.buffer.head_at_end();
            declare_done =
                streaming_from.buffer.is_done() || (complete_allow_partial && head_at_end);

            let batch_end =
                u16::from(packet.fec_info.batch_pos) + 1 == u16::from(packet.fec_info.batch_size);

            score_ranges = if declare_done {
                vec![]
            } else {
                let fec_active = fec.active_byte_ranges();
                if head_at_end {
                    streaming_from.finalize_sweep_pick(&fec_active)
                } else if batch_end {
                    streaming_from.score_policy_pick(&fec_active)
                } else {
                    vec![]
                }
            };

            address_copy = *lock_read!(address);
        } else {
            return Err(Error::StateMismatch {
                expected: FlatState::StreamingFrom,
                found: (&*self).into(),
            });
        }

        if declare_done {
            // TODO: key rotation
            debug!("buffer complete");

            Box::new(PlaybackControlPacket::done(
                Options::construct(&[OptionFlags::RequireAck]),
                packet.session_id,
            ))
            .send(sender.clone(), address_copy)
            .await;

            self.close_stream().await;

            return Ok(false);
        }

        dispatch_retransmit_request(&sender, packet.session_id, address_copy, score_ranges).await;

        Ok(fec::received(packet).await)
    }

    pub async fn address(&self) -> SocketAddr {
        match self {
            ConnectionStates::Handshake { address, .. } => *lock_read!(address),
            ConnectionStates::Established(established) => *lock_read!(established.address),
        }
    }

    // TODO:
    /// # Errors
    pub async fn recovered_packet(
        &mut self,
        recovered: Recovered,
        sender: ManagerToProcessor,
    ) -> ErrResult {
        let address_copy: SocketAddr;
        let session_id_copy: SessionId;
        let declare_done: bool;
        let score_ranges: Vec<ByteRange>;
        if let ConnectionStates::Established(established) = self
            && let EstablishedState {
                state:
                    SessionStates::Streaming(StreamState {
                        streaming: Streaming::From(streaming_from),
                        stream,
                        fec,
                    }),
                address,
                session_id,
                ..
            } = established.as_mut()
        {
            // Site 7: emit recovery observations *before* eviction, since
            // `batch_started_at` reads the mirror entry that `evict` removes.
            // `recovered.packets.len()` is the count of reconstructed packets;
            // latency is local-clock-only (peer epochs are not comparable).
            let dc = get_state!().data_collection.clone();
            if let Ok(recovered_count) = u32::try_from(recovered.packets.len())
                && recovered_count > 0
            {
                dc.post(Observation::PacketsRecovered {
                    session: *session_id,
                    count: recovered_count,
                });
            }
            if let Some(start) = fec.batch_started_at(recovered.batch_id) {
                let now = Timestamp::now();
                let latency = now.get().saturating_sub(start.get());
                dc.post(Observation::BatchRecovered {
                    session: *session_id,
                    latency_ms: u32::try_from(latency).unwrap_or(u32::MAX),
                });
            }

            // Batch is settled — drop the manager-side mirror entry.
            fec.evict(recovered.batch_id);

            stream.send_modify(|m| _ = m.approved.replace(true));

            for packet in recovered.packets {
                let pos = packet.byte_range_start;
                streaming_from
                    .buffer
                    .write(pos, packet.payload)
                    .map_err(|e| {
                        if matches!(e, BufferError::FailedToDeref) {
                            Error::FailedToDeref
                        } else {
                            Error::IrrelevantError
                        }
                    })?;
                streaming_from.clear_pending(pos);
                stream.send_modify(|m| {
                    m.head = streaming_from.buffer.head();
                });
            }
            let complete_allow_partial = stream.borrow().complete.is_some_and(identity);
            let head_at_end = streaming_from.buffer.head_at_end();
            declare_done =
                streaming_from.buffer.is_done() || (complete_allow_partial && head_at_end);

            // Recovery completing a batch is just as good a trigger as a
            // data batch_end. Same two-mode policy as `received_data_packet`:
            // finalize-sweep when head_at_end, score-policy otherwise.
            score_ranges = if declare_done {
                vec![]
            } else {
                let fec_active = fec.active_byte_ranges();
                if head_at_end {
                    streaming_from.finalize_sweep_pick(&fec_active)
                } else {
                    streaming_from.score_policy_pick(&fec_active)
                }
            };

            address_copy = *lock_read!(address);
            session_id_copy = *session_id;
        } else {
            return Err(Error::StateMismatch {
                expected: FlatState::StreamingFrom,
                found: (&*self).into(),
            });
        }

        if declare_done {
            debug!("buffer complete");
            Box::new(PlaybackControlPacket::done(
                Options::construct(&[OptionFlags::RequireAck]),
                session_id_copy,
            ))
            .send(sender, address_copy)
            .await;
            if let ConnectionStates::Established(established) = self
                && let EstablishedState {
                    state: SessionStates::Streaming(StreamState { stream, .. }),
                    ..
                } = established.as_ref()
            {
                stream.send_modify(|m| {
                    m.closed = true;
                });
            }
            return Ok(());
        }

        dispatch_retransmit_request(&sender, session_id_copy, address_copy, score_ranges).await;

        Ok(())
    }

    pub async fn update_address(&self, new: SocketAddr) {
        match self {
            ConnectionStates::Handshake { address, .. } => *lock_write!(address) = new,
            ConnectionStates::Established(established) => *lock_write!(established.address) = new,
        }
    }

    pub fn session_id(&self) -> SessionId {
        match self {
            ConnectionStates::Handshake { session_id, .. } => *session_id,
            ConnectionStates::Established(established) => established.session_id,
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
    pub ack: Arc<PendingAckWindow>,
    pub encryption: EncryptionTable,
    pub fingerprints: FingerprintWindow,
    pub address_session: AddressSessionIdTable,
    pub fec_prune: Arc<FecPruneTask>,
    /// Fire-and-forget sender every layer can post observations to. Distributed
    /// at init via `get_state!()`; cloned freely.
    pub data_collection: DataCollectionChannel,
    /// App-side handle used to pull accumulated entries.
    pub data_drain: DrainHandle,
}

impl ProtocolState {
    #[must_use]
    pub fn new(port: Port, app_id: AppId, sender: ManagerToProcessor) -> Self {
        let (data_collection, data_drain) = start_data_collector();
        Self {
            app_id,
            port,
            handles: Mutex::default(),
            global_handle_monitor: Arc::default(),
            connections: ConnectionStatesTable::default(),
            handshakes: HandshakeStateTable::default(),
            ack: Arc::new(PendingAckWindow::new(sender.clone())),
            encryption: EncryptionTable::default(),
            fingerprints: FingerprintWindow::default(),
            address_session: AddressSessionIdTable::default(),
            fec_prune: Arc::new(FecPruneTask::new(sender)),
            data_collection,
            data_drain,
        }
    }

    /// Joins both layer threads.
    pub async fn join_layers(&mut self) {
        let handles = o_unwrap_or_return!(lock!(self.handles).take().panic_in_debug(
            "Invariant broken while joining the layer threads: \
            function was called more than once",
        ));

        _ = handles.join().await;
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

    pub async fn set_handles(
        &self,
        transport: JoinHandle<ErrResult>,
        processor: JoinHandle<ErrResult>,
    ) {
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
        connection: mpsc::Sender<InnerConnectionEvent>,
        app_id: AppId,
    ) -> Option<(
        EphemeralSecret,
        oneshot::Sender<
            core::result::Result<
                (SessionId, mpsc::Receiver<InnerConnectionEvent>),
                ConnectionError,
            >,
        >,
    )> {
        let HandshakeState {
            ephemeral_secret,
            session_id,
            response,
            ..
        } = self.handshakes.take(handshake_id).await?;

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
            session_id,
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
        match o_unwrap_or_return!(lock.remove(&session_id)) {
            ConnectionStates::Handshake {
                session_id,
                ack_triggered_response,
                app_id,
                address,
                signal,
            } => {
                let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);
                lock.insert(
                    session_id,
                    ConnectionStates::Established(Box::new(EstablishedState {
                        session_id,
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
            established @ ConnectionStates::Established { .. } => {
                lock.insert(session_id, established);
            }
        }
    }

    pub async fn new_session(
        &self,
        session_id: SessionId,
        response: oneshot::Sender<
            core::result::Result<
                (SessionId, mpsc::Receiver<InnerConnectionEvent>),
                ConnectionError,
            >,
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
            session_id,
            ack_triggered_response: response,
            app_id,
            signal: watch::channel(false).0,
            address: RwLock::new(address),
        });

        lock_write!(self.address_session)
            .entry(address)
            .and_modify(|v| v.push(session_id))
            .or_insert(vec![session_id]);
    }

    pub async fn advertise_closed(&self) {
        for (session_id, connection) in lock_write!(self.connections).drain() {
            match connection {
                ConnectionStates::Handshake {
                    session_id,
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
                ConnectionStates::Established(established) => {
                    if established
                        .connection
                        .send(InnerConnectionEvent::ProtocolClosed)
                        .await
                        .is_err()
                    {
                        debug_assert!(
                            false,
                            "Invariant broken in `advertise_closed`: \
                                channel to connection with session ID {session_id} was already closed.",
                        );
                    }
                }
            }
        }
    }

    // `clippy::too_many_lines`: this is the sender-side stream driver — the
    // top-level setup, a long-running `select!`, the per-event arms (pause,
    // play, seek, retransmit, lifecycle), and the per-tick send dispatch all
    // share the same async context. Splitting would force the long-lived
    // local state (playing/extras/last_sent) into separate signatures.
    #[allow(clippy::too_many_lines)]
    #[instrument(skip_all)]
    pub async fn send_on_session(
        &self,
        session_id: SessionId,
        buffer: ReadableBuffer,
        sender: watch::Sender<StreamMessage>,
        outbound_sender: ManagerToProcessor,
        fec_config: FecConfig,
    ) {
        let event = {
            let mut lock = lock_write!(get_state!().connections);
            let session = o_unwrap_or_return!(lock.get_mut(&session_id).panic_in_debug(&format!(
                "Invariant broken in `send_on_session` \
                with ID {session_id}: session does not exist"
            )));

            session.stream_to(buffer, sender, fec_config);

            if let ConnectionStates::Established(established) = session
                && let EstablishedState {
                    state:
                        SessionStates::Streaming(StreamState {
                            streaming: Streaming::To(StreamingTo { event, .. }),
                            ..
                        }),
                    address,
                    ..
                } = established.as_ref()
            {
                // Site 10 (open, sender side): we just transitioned this
                // session to `StreamingTo`. Stamp the identity row.
                let remote = *lock_read!(address);
                get_state!()
                    .data_collection
                    .post(Observation::SessionOpened {
                        session: session_id,
                        local_addr_hash: hash_local_port(*get_state!().port()),
                        remote_addr_hash: hash_addr(&remote),
                        fec_config,
                    });
                event.clone()
            } else {
                debug_assert!(
                    false,
                    "Invariant broken in `send_on_session` \
                    with ID {session_id}: session not in correct state even though stream_to() was just called"
                );
                return;
            }
        };

        let mut playing = true;
        let mut interval = interval(SEND_INTERVAL);
        let mut extras: BTreeSet<usize> = std::collections::BTreeSet::new();
        let mut last_sent = BytePosition(0);

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
                            StreamEvent::Playback(PlaybackControl::Pause) => {
                                playing = false;
                                // Site 12: sender-side flag flip.
                                get_state!()
                                    .data_collection
                                    .post(Observation::Paused { session: session_id });
                            }
                            StreamEvent::Playback(PlaybackControl::Play) => {
                                playing = true;
                            },

                            StreamEvent::Playback(PlaybackControl::Seek(to)) => {
                                debug!("got seek request to position {to}");
                                seek_action(session_id, to).await;
                                add_seek_hole_indices(&mut extras, last_sent, to);
                                // Site 12: sender-side flag flip.
                                get_state!()
                                    .data_collection
                                    .post(Observation::Seeked { session: session_id });
                            }

                            StreamEvent::Playback(PlaybackControl::Done) => {
                                debug!("stream done for session {session_id}");
                                self.close_stream(session_id, outbound_sender.clone()).await;
                                return;
                            }

                            StreamEvent::Playback(PlaybackControl::Close) => {
                                debug!("Close received for session {session_id}");
                                close_outgoing_stream_action(session_id).await;
                                return;
                            }

                            StreamEvent::Retransmit(byte_ranges) => {
                                for r in byte_ranges.into_iter()
                                    .flat_map(split_range_into_chunks)
                                {
                                    extras.insert((*r.start as usize) / MAX_PAYLOAD_LENGTH);
                                }
                            }
                        }
                    }
                _ = interval.tick(), if playing => {
                    // Site 9 (send tick): emit one observation per tick with
                    // a flag for whether anything was actually shipped. The
                    // sender-side `BatchFillLatency` documented at this site
                    // collapses to the tick interval (each tick produces
                    // exactly one batch in the current architecture), so we
                    // don't emit it separately — derive it as
                    // (window_ms / send_ticks_busy) post-hoc if needed.
                    let result = send_stream_action(
                        session_id,
                        outbound_sender.clone(),
                        PACKET_COUNT_PER_BATCH,
                        &mut extras,
                    ).await;
                    get_state!().data_collection.post(Observation::SendTick {
                        session: session_id,
                        emitted: result.is_some(),
                    });
                    last_sent = result.unwrap_or(last_sent);
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
            ConnectionStates::Established(established) => *lock_read!(established.address),
        };

        // Site 10 (close): post SessionClosed regardless of how we exited.
        // Harmless for sessions that never reached SessionOpened — the
        // collector drops observations for unknown sessions.
        get_state!()
            .data_collection
            .post(Observation::SessionClosed {
                session: session_id,
            });

        o_unwrap_or_return!(
            get_state!()
                .address_session
                .remove(address, session_id)
                .await
        );

        lock_write!(get_state!().encryption).remove(&session_id);
    }

    pub async fn close_stream(&self, session_id: SessionId, _outbound_sender: ManagerToProcessor) {
        let mut lock = lock_write!(self.connections);
        let session = o_unwrap_or_return!(lock.get_mut(&session_id));

        if let ConnectionStates::Established(established) = session
            && let EstablishedState { state, .. } = established.as_mut()
        {
            match state {
                SessionStates::Streaming(
                    StreamState {
                        streaming: Streaming::To(StreamingTo { event, .. }),
                        stream,
                        ..
                    },
                    ..,
                ) => {
                    event
                        .update_with(|e| *e = StreamEvent::Playback(PlaybackControl::Close))
                        .await;
                    stream.send_modify(|m| m.closed = true);
                }
                SessionStates::Streaming(StreamState { stream, .. }) => {
                    stream.send_modify(|m| m.closed = true);
                }
                _ => {
                    return;
                }
            }

            *state = SessionStates::Up;

            // Site 10 (close, stream): the session lives on but its data-flow
            // window has ended; emit so the collector flushes the open entry
            // and frees its scratch state for this session.
            get_state!()
                .data_collection
                .post(Observation::SessionClosed {
                    session: session_id,
                });
        }
    }

    pub async fn update_address(
        &self,
        session_id: SessionId,
        new: SocketAddr,
    ) -> Option<SocketAddr> {
        let curr = {
            let curr = lock_read!(self.connections)
                .get(&session_id)?
                .address()
                .await;

            (new != curr).then_some(())?;

            curr
        };

        get_state!()
            .address_session
            .move_session(curr, session_id, new)
            .await
            .panic_in_debug(&format!("Invariant broken in `update_address`:\
                session ID {session_id} was in the connections table, but its current address ({curr})\
                could not be found"))?;

        lock_write!(get_state!().connections)
            .get_mut(&session_id)?
            .update_address(new)
            .await;

        // Site 11: address actually changed (the `curr != new` guard above
        // ruled out no-ops). One-bit flag for the entry.
        get_state!()
            .data_collection
            .post(Observation::AddressRebind {
                session: session_id,
            });

        Some(new)
    }

    pub async fn send_keep_alive_on_session(
        &self,
        session_id: SessionId,
        sender: ManagerToProcessor,
    ) {
        loop {
            tokio_sleep(KEEP_ALIVE_INTERVAL).await;
            let addr = o_unwrap_or_return!(
                lock_read!(get_state!().connections)
                    .get(&session_id)
                    .log_debug(&format!(
                        "session with ID {session_id} closed, stopping keepalive"
                    ))
            )
            .address()
            .await;
            Box::new(KeepAlivePacket::new(Options::none(), session_id))
                .send(sender.clone(), addr)
                .await;
        }
    }
}

async fn seek_action(session_id: SessionId, pos: BytePosition) {
    if let Some(ConnectionStates::Established(established)) =
        lock_write!(get_state!().connections).get_mut(&session_id)
        && let EstablishedState {
            state:
                SessionStates::Streaming(StreamState {
                    streaming: Streaming::To(StreamingTo { buffer, .. }),
                    ..
                }),
            ..
        } = established.as_mut()
    {
        buffer.seek(pos);
    }
}

#[instrument(skip_all)]
async fn send_stream_action(
    session_id: SessionId,
    outbound_sender: ManagerToProcessor,
    n: usize,
    extras: &mut std::collections::BTreeSet<usize>,
) -> Option<BytePosition> {
    let action = {
        let mut lock = lock_write!(get_state!().connections);
        let session = lock.get_mut(&session_id)?;
        if let ConnectionStates::Established(established) = session
            && let EstablishedState {
                state:
                    SessionStates::Streaming(
                        stream @ StreamState {
                            streaming: Streaming::To(_),
                            ..
                        },
                    ),
                address,
                ..
            } = established.as_mut()
            && let Some(chunks) = stream.get_chunks(n)
        {
            // Head sends always win the tick budget. Only fall through to
            // extras (seek-skipped + retransmit-requested chunk indices)
            // when fresh data is fully exhausted, and even then drain at
            // most `n` chunks so retransmits inherit the same pacing.
            let chunks = if chunks.is_empty() && !extras.is_empty() {
                debug!("draining {} extras (budget {})", extras.len().min(n), n);
                let take_idxs: Vec<usize> = extras.iter().take(n).copied().collect();
                for i in &take_idxs {
                    extras.remove(i);
                }
                #[allow(clippy::cast_possible_truncation)]
                let to_send: Vec<ByteRange> = take_idxs
                    .into_iter()
                    .map(|i| {
                        ByteRange::new(
                            BytePosition((i * MAX_PAYLOAD_LENGTH) as u32),
                            MAX_PAYLOAD_LENGTH as u16,
                        )
                    })
                    .collect();
                stream.retransmit(to_send)?
            } else {
                chunks
            };

            if chunks.is_empty() {
                debug!("buffer complete, nothing to send");
                return None;
            }

            let addr = *lock_read!(address);
            let (current_batch, fec_config) =
                if let Streaming::To(streaming_to) = &mut stream.streaming {
                    (streaming_to.next_batch_id(), streaming_to.fec_config)
                } else {
                    debug!("session {session_id} not Streaming::To after check");
                    return None;
                };
            debug!(
                "session {session_id} batch {current_batch}: {} chunks ",
                chunks.len(),
            );
            Some((chunks, addr, current_batch, fec_config))
        } else {
            debug!(
                "state is wrong or buffer has been fully sent for session {session_id} - no action"
            );
            None
        }
    };

    if let Some((chunks, addr, current_batch, fec_config)) = action {
        let last = chunks.last()?.0;
        send_data_packets(
            chunks,
            &outbound_sender,
            session_id,
            addr,
            current_batch,
            fec_config,
        )
        .await;
        Some(last)
    } else {
        None
    }
}

async fn close_outgoing_stream_action(session_id: SessionId) {
    debug!("close_outgoing_stream_action: session {session_id}");
    let mut lock = lock_write!(get_state!().connections);
    let _session = o_unwrap_or_return!(lock.get_mut(&session_id).panic_in_debug(&format!(
        "Invariant broken while trying to send on a session \
                            with ID {session_id}: session does not exist"
    )));
}

#[allow(clippy::cast_possible_truncation)]
async fn send_data_packets(
    chunks: Vec<(BytePosition, Box<[u8]>)>,
    outbound_sender: &ManagerToProcessor,
    session_id: SessionId,
    address: SocketAddr,
    current_batch: BatchID,
    fec_config: FecConfig,
) {
    let len = chunks.len() as u8;
    debug!("send_data_packets: session {session_id} batch {current_batch}: {len} packets");
    for (i, (position, payload)) in chunks.into_iter().enumerate() {
        let packet = DataPacket::new(
            Options::none(),
            current_batch,
            FECInfo {
                batch_size: len,
                batch_pos: i as u8,
                recovery_count: fec_config.recovery_count.min(len),
                scheme: fec_config.scheme,
            },
            session_id,
            position,
            payload,
        );
        Box::new(packet)
            .send(outbound_sender.clone(), address)
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
                if let Some(ConnectionStates::Established(established)) = connections.get(*session)
                    && matches!(established.state, SessionStates::Up | SessionStates::Down)
                {
                    true
                } else {
                    false
                }
            })
            .copied()
    }

    pub async fn move_session(
        &self,
        source_address: SocketAddr,
        session_id: SessionId,
        destination_address: SocketAddr,
    ) -> Option<SocketAddr> {
        _ = self.remove(source_address, session_id).await?;
        lock_write!(self)
            .entry(destination_address)
            .and_modify(|v| v.push(session_id))
            .or_insert(vec![session_id]);
        Some(destination_address)
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
    transport: JoinHandle<ErrResult>,
    processor: JoinHandle<ErrResult>,
}

impl LayerHandles {
    /// Joins both layers
    async fn join(self) -> ErrResult {
        self.processor
            .await
            .map_err(|_| Error::Task(TaskError::TaskFailed))??;
        self.transport
            .await
            .map_err(|_| Error::Task(TaskError::TaskFailed))??;
        Ok(())
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
        core::result::Result<(SessionId, mpsc::Receiver<InnerConnectionEvent>), ConnectionError>,
    >,
}

impl HandshakeState {
    #[must_use]
    pub fn new(
        peer_address: SocketAddr,
        ephemeral_secret: EphemeralSecret,
        session_id: SessionId,
        response: oneshot::Sender<
            core::result::Result<
                (SessionId, mpsc::Receiver<InnerConnectionEvent>),
                ConnectionError,
            >,
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
            core::result::Result<
                (SessionId, mpsc::Receiver<InnerConnectionEvent>),
                ConnectionError,
            >,
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

/// Manager-side mirror of which batches are currently in flight for a single
/// session. Built from the `DataPacket`s the manager already receives — no
/// state is bled across the FEC module boundary. The score policy uses this
/// to skip chunks still actively being received via FEC's primary path.
///
/// No internal lock: always accessed via `&mut StreamState` under the outer
/// connections write lock.
#[derive(Default, Debug)]
pub struct SessionFecState {
    table: HashMap<BatchID, FecBatchWindow>,
}

impl SessionFecState {
    /// Time a batch can sit in the manager-side mirror before being assumed
    /// stale and evicted on next read. Same as the FEC-internal TTL — these
    /// two clocks are in lockstep semantically.
    pub const ENTRY_TTL_MS: u64 = PACKET_DISCARD_TIME_MS;

    pub fn add_data(
        &mut self,
        batch_id: BatchID,
        fec_info: FECInfo,
        byte_range_start: BytePosition,
    ) {
        let FECInfo {
            batch_size,
            batch_pos,
            recovery_count,
            ..
        } = fec_info;
        self.table
            .entry(batch_id)
            .or_insert_with(|| FecBatchWindow::new(batch_size, recovery_count))
            .add_data(batch_pos as usize, byte_range_start);
    }

    /// Drop the manager's mirror entry for `batch_id`. Called when the
    /// receiver knows the batch is settled (e.g. successful recovery).
    pub fn evict(&mut self, batch_id: BatchID) {
        self.table.remove(&batch_id);
    }

    /// Local-clock time the manager first observed this batch. Used by the
    /// data-collection layer to compute recovery latency at site 7.
    #[must_use]
    pub fn batch_started_at(&self, batch_id: BatchID) -> Option<Timestamp> {
        self.table.get(&batch_id).map(|b| b.created_at)
    }

    /// Byte ranges currently held in active batches — chunks the score
    /// policy should not request, since they're still expected via the
    /// primary path. Sweeps stale entries (older than `ENTRY_TTL_MS`)
    /// before computing.
    #[allow(clippy::cast_possible_truncation)]
    pub fn active_byte_ranges(&mut self) -> Vec<Range<usize>> {
        self.table
            .retain(|_, batch| !batch.created_at.been_longer_than(Self::ENTRY_TTL_MS));
        self.table
            .values()
            .filter_map(FecBatchWindow::contiguous_byte_range)
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct FecBatchWindow {
    batch_size: u8,
    recovery_count: u8,
    created_at: Timestamp,
    base_byte_pos: Option<BytePosition>,
    is_contiguous: bool,
}

impl FecBatchWindow {
    fn new(batch_size: u8, recovery_count: u8) -> Self {
        Self {
            batch_size,
            recovery_count,
            created_at: Timestamp::now(),
            base_byte_pos: None,
            is_contiguous: true,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    #[inline]
    fn add_data(&mut self, index: usize, byte_range_start: BytePosition) {
        let derived_base = BytePosition(
            byte_range_start
                .0
                .saturating_sub((index as u32) * MAX_PAYLOAD_LENGTH as u32),
        );
        match self.base_byte_pos {
            None => self.base_byte_pos = Some(derived_base),
            Some(existing) if existing.0 != derived_base.0 => {
                self.is_contiguous = false;
            }
            _ => {}
        }
    }

    /// `[base, base + batch_size * MAX_PAYLOAD_LENGTH)` for contiguous batches
    /// where at least one data packet has set the base. Non-contiguous /
    /// parity-only batches return `None`.
    #[allow(clippy::cast_possible_truncation)]
    fn contiguous_byte_range(&self) -> Option<Range<usize>> {
        if !self.is_contiguous {
            return None;
        }
        let base = self.base_byte_pos?.0 as usize;
        Some(base..base + (self.batch_size as usize) * MAX_PAYLOAD_LENGTH)
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

    pub fn add(&self, packet: Packet) {
        self.table.add(packet);
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
    table: &'static FingerprintWindow,
}

impl FingerprintMonitor {
    pub fn new(table: &'static FingerprintWindow) -> Self {
        Self { table }
    }

    pub async fn add(&self, fingerprint: PacketFingerprint) -> bool {
        let fingerprint = fingerprint.into();
        self.table.add(fingerprint).await
    }

    pub async fn contains(&self, fingerprint: &PacketFingerprint) -> bool {
        self.table.contains(fingerprint).await
    }
}

struct PendingAckQueueEntry {
    timestamp: Timestamp,
    fingerprint: PacketFingerprint,
    retries: u8,
}

impl PendingAckQueueEntry {
    const MAX_RETRIES: u8 = 5;
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;

    fn new(fingerprint: PacketFingerprint) -> Self {
        Self {
            timestamp: Timestamp::now(),
            fingerprint,
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
    const BUFFERING_TIME: u64 = 100;

    #[must_use]
    pub fn new(sender: ManagerToProcessor) -> Self {
        Self {
            pending: RwLock::default(),
            queue: Mutex::default(),
            sender,
            canceled: AtomicBool::new(false),
        }
    }

    pub fn init(self: Arc<Self>) {
        get_state!().global_handle_monitor.dispatch(self.prune());
    }

    pub fn add(&'static self, packet: Packet) {
        if self.canceled.load(Ordering::Relaxed) {
            return;
        }
        get_state!()
            .global_handle_monitor
            .clone()
            .dispatch(self.inner_add(packet));
    }

    pub async fn acknowledge(&self, fingerprint: impl Into<PacketFingerprint>) {
        if self.canceled.load(Ordering::Relaxed) {
            return;
        }
        let fingerprint = fingerprint.into();
        lock_write!(self.pending).remove(&fingerprint);
    }

    async fn inner_add(&self, mut packet: Packet) {
        let fingerprint = r_unwrap_or_return!(PacketFingerprint::try_from(&packet).panic_in_debug(
            &format!(
                "Invariant broken while adding a packet to `PendingAckWindow`:\
                A packet that should not be acked was provided ({packet:?}) full list can\
                be found at the impl TryFrom<&Packet> for PacketFingerprint",
            )
        ));
        packet.mark_resend();

        let entry = PendingAckQueueEntry::new(fingerprint.clone());
        lock_write!(self.pending).insert(fingerprint, packet);
        lock!(self.queue).push_back(entry);
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired: Vec<PacketFingerprint> = Vec::with_capacity(256);
        let mut to_retry = Vec::with_capacity(256);
        let mut top_timestamp = Self::PRUNE_INTERVAL - Self::BUFFERING_TIME;

        while !self.canceled.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(top_timestamp + Self::BUFFERING_TIME)).await;
            top_timestamp = {
                // get expired pending ack packets as well as ones to retry
                let mut queue = lock!(self.queue);
                while queue
                    .front()
                    .is_some_and(PendingAckQueueEntry::ready_to_retry)
                {
                    if let Some(mut value) = queue.pop_front() {
                        if value.retried() {
                            expired.push(value.fingerprint);
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
            for entry in to_retry.drain(..) {
                let packet = {
                    let pending = lock_read!(self.pending);
                    let Some(packet) = pending.get(&entry.fingerprint) else {
                        continue;
                    };
                    packet.clone()
                };

                let session_id = o_unwrap_or_return!(packet.session_id().panic_in_debug(&format!(
                    "A packet that should never be acked has been inserted: {packet:?}"
                )));

                let addr = {
                    let lock = lock_read!(get_state!().connections);
                    if let Some(ConnectionStates::Established(established)) = lock.get(&session_id)
                        && let EstablishedState { address, .. } = established.as_ref()
                    {
                        *lock_read!(address)
                    } else {
                        continue;
                    }
                };

                lock!(self.queue).push_back(entry);

                Box::new(packet).send(self.sender.clone(), addr).await;
            }

            // remove expired acks
            {
                let mut pending = lock_write!(self.pending);
                expired
                    .drain(..)
                    .for_each(|fingerprint| _ = pending.remove(&fingerprint));
            }
        }
    }

    pub fn close(&self) {
        self.canceled.store(true, Ordering::Release);
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

    pub fn init(self: Arc<Self>) {
        get_state!().global_handle_monitor.dispatch(self.prune());
    }

    #[must_use]
    pub async fn contains(&self, fingerprint: &PacketFingerprint) -> bool {
        lock_read!(self.fingerprints).contains(fingerprint)
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

        lock!(self.queue).push_back((Timestamp::now(), ptr));

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

/// Long-running task that periodically asks FEC to prune inbound batches
/// that have aged out without becoming recoverable, and emits retransmit
/// requests for each pruned batch's missing chunks.
pub struct FecPruneTask {
    sender: ManagerToProcessor,
    canceled: AtomicBool,
}

impl FecPruneTask {
    /// A batch can sit in FEC inbound state this long before it's declared stuck.
    const TTL_MS: u64 = PACKET_DISCARD_TIME_MS;
    /// Sleep between sweeps. Smaller than TTL so a stuck batch is caught
    /// within ~`SLEEP_MS` of crossing the threshold.
    const SLEEP: Duration = Duration::from_millis(100);

    #[must_use]
    pub fn new(sender: ManagerToProcessor) -> Self {
        Self {
            sender,
            canceled: AtomicBool::new(false),
        }
    }

    pub fn init(self: Arc<Self>) {
        get_state!().global_handle_monitor.dispatch(self.prune());
    }

    pub fn close(&self) {
        self.canceled.store(true, Ordering::Release);
    }

    pub async fn prune(self: Arc<Self>) {
        while !self.canceled.load(Ordering::Relaxed) {
            tokio::time::sleep(Self::SLEEP).await;

            let expired = fec::prune(Self::TTL_MS).await;

            // Site 8: each expired tuple with non-empty missing_positions is
            // a batch FEC couldn't reconstruct — emit one BatchUnrecoverable
            // before merging positions for the retransmit-fallback path.
            // Empty missing_positions means the batch pruned clean (already
            // complete from data alone); not an unrecoverable event.
            let dc = get_state!().data_collection.clone();
            for (session_id, _batch_id, positions) in &expired {
                if !positions.is_empty() {
                    dc.post(Observation::BatchUnrecoverable {
                        session: *session_id,
                    });
                }
            }

            // Group by session so the connections write lock is acquired
            // once per session, regardless of how many batches expired for it.
            let mut by_session: HashMap<SessionId, Vec<BytePosition>> = HashMap::default();
            for (session_id, _batch_id, positions) in expired {
                if positions.is_empty() {
                    continue;
                }
                by_session.entry(session_id).or_default().extend(positions);
            }

            for (session_id, positions) in by_session {
                // Resolve session address + dedup the positions through the
                // session's StreamingFrom.pending. Skip if the session is
                // gone or no positions survive the dedup filter.
                let dispatch = {
                    let mut lock = lock_write!(get_state!().connections);
                    let Some(state) = lock.get_mut(&session_id) else {
                        continue;
                    };
                    if let ConnectionStates::Established(established) = state
                        && let EstablishedState {
                            state:
                                SessionStates::Streaming(StreamState {
                                    streaming: Streaming::From(streaming_from),
                                    ..
                                }),
                            address,
                            ..
                        } = established.as_mut()
                    {
                        let accepted = streaming_from.reserve_for_request(positions);
                        if accepted.is_empty() {
                            None
                        } else {
                            Some((accepted, *lock_read!(address)))
                        }
                    } else {
                        None
                    }
                };
                let Some((accepted, addr)) = dispatch else {
                    continue;
                };

                let ranges = coalesce_byte_positions(accepted);
                dispatch_retransmit_request(&self.sender, session_id, addr, ranges).await;
            }
        }
    }
}

/// Insert chunk indices covering `[low, high)` into `extras`. Duplicate
/// indices are absorbed by the set.
#[allow(clippy::cast_possible_truncation)]
fn add_seek_hole_indices(
    extras: &mut std::collections::BTreeSet<usize>,
    last_sent: BytePosition,
    to: BytePosition,
) {
    let (lo, hi) = if to.0 > last_sent.0 {
        (last_sent.0 as usize, to.0 as usize)
    } else {
        (to.0 as usize, last_sent.0 as usize)
    };
    let start_chunk = lo / MAX_PAYLOAD_LENGTH;
    let end_chunk = hi.div_ceil(MAX_PAYLOAD_LENGTH);
    for i in start_chunk..end_chunk {
        extras.insert(i);
    }
}

/// Break a (possibly large) `ByteRange` into chunk-sized `ByteRange`s of
/// length `MAX_PAYLOAD_LENGTH` (last one possibly shorter). Used so the
/// run-streaming loop can keep a flat per-chunk queue and the per-tick budget
/// is just a `drain(..n)`.
#[allow(clippy::cast_possible_truncation)]
fn split_range_into_chunks(range: ByteRange) -> impl Iterator<Item = ByteRange> {
    let start = range.start.0;
    let end = start + range.length as u32;
    let mut pos = start;
    std::iter::from_fn(move || {
        if pos >= end {
            return None;
        }
        let len = (end - pos).min(MAX_PAYLOAD_LENGTH as u32) as u16;
        let r = ByteRange::new(BytePosition(pos), len);
        pos += len as u32;
        Some(r)
    })
}

/// Send `RetransmitPacket`s carrying the given ranges, splitting into multiple
/// packets when the payload exceeds `RetransmitPacket::LOCAL_MAX_PAYLOAD_LENGTH`.
async fn dispatch_retransmit_request(
    sender: &ManagerToProcessor,
    session_id: SessionId,
    addr: SocketAddr,
    ranges: Vec<ByteRange>,
) {
    if ranges.is_empty() {
        return;
    }
    let max_per_packet = RetransmitPacket::LOCAL_MAX_PAYLOAD_LENGTH / ByteRange::elem_size();
    for chunk in ranges.chunks(max_per_packet) {
        Box::new(RetransmitPacket::data(
            Options::none(),
            session_id,
            chunk.to_vec(),
        ))
        .send(sender.clone(), addr)
        .await;
    }
}

/// Merge a sorted list of single-chunk byte positions into the smallest set
/// of `ByteRange`s that covers them, coalescing contiguous runs.
#[allow(clippy::cast_possible_truncation)]
fn coalesce_byte_positions(positions: Vec<BytePosition>) -> Vec<ByteRange> {
    let mut iter = positions
        .into_iter()
        .map(|p| ByteRange::new(p, MAX_PAYLOAD_LENGTH as u16));
    let Some(mut current) = iter.next() else {
        return vec![];
    };
    let mut out = vec![];
    for next in iter {
        if !current.concat(&next) {
            out.push(current);
            current = next;
        }
    }
    out.push(current);
    out
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
