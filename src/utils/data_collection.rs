//! Per-session data collection for offline regression-model training.
//!
//! All layers feed observations to a single collector task through an
//! `mpsc::UnboundedSender`; the collector folds them into 500 ms windows per
//! session and stores completed entries until the app drains them through
//! [`DrainHandle`]. Sends never block (`UnboundedSender::send` is sync), which
//! satisfies the "data collection must be invisible" rule by keeping hot
//! protocol paths off any I/O.
//!
//! Layers do not access each other's state — they only post `Observation`
//! values. The collector owns the only data-collection state.
//!
//! ### Field philosophy
//! The struct is intentionally wide. It is cheaper to drop columns when
//! cleansing a CSV than to re-run an experiment, so we record raw counters
//! and sample sums/squares rather than pre-computed ratios wherever the
//! reconstruction is trivial. The training pipeline can derive throughput,
//! loss rate, mean/stddev RTT, etc. from these.
//!
//! ### Recording points
//!
//! Every emission site in the protocol that posts to
//! [`DataCollectionChannel`]. Kept deliberately small — each new site is a
//! place future changes can drift, and a single site can emit multiple
//! observation kinds at once for the same packet/event. Sites are numbered
//! for cross-reference in commit messages.
//!
//! | # | Site (`file::fn`) | Layer | Observation(s) emitted | Per-event cost |
//! |---|-----------------|-------|------------------------|----------------|
//! | 1 | `packet_processor/outbound.rs` — `DataPacket` arm + `handle_parity` | processor | `BytesSent` + {`DataPacketSent` \| `ParityPacketSent`}. Other packet kinds (acks, keepalives, control) are not emitted — they don't feed the training features. | 2 channel sends per data/parity packet |
//! | 2 | `packet_processor/inbound.rs::handle_packet` — post-decode match | processor | `BytesReceived` + {`DataPacketReceived` \| `ParityPacketReceived`} for data/parity only. The collector synthesises inter-arrival ms (sum/sumsq/count) and `last_activity_gap_ms` from `DataPacketReceived` timing in its per-session scratch, so the emitter does **not** stamp a timestamp. | 2 channel sends per data/parity packet |
//! | 3 | `manager/state.rs::ConnectionStates::received_data_packet` (called from `manager/routines/received.rs::received_data_packet`) | manager | `ReorderDistance` (packs `(batch_id, batch_pos)` into a u32 seqno, compares to per-session max-seen on `StreamingFrom`); `BufferState { head, len }` after the buffer write. | 1–2 channel sends per packet |
//! | 4 | `manager/routines/received.rs::received_retransmit_request` | manager | `RetransmitServed { count: packet.payload.len() }`. `RetransmitSent` is **not** separately emitted — actual re-emits flow back through site 1's `DataPacketSent`, so the column can be approximated post-hoc from co-occurring served/sent counts. | 1 channel send per request |
//! | 5 | `manager/state.rs::StreamingFrom::reserve_for_request` | manager | `HolesObserved` (count of unfilled positions seen this call, incl. ones already pending), `RetransmitIssued` (post-filter accepted count), `LossBurst` per terminated run of consecutive accepted chunks. Single iteration handles all three. | 1–3 channel sends per call |
//! | 6 | `manager/state.rs::StreamingFrom::clear_pending` | manager | `RetransmitRtt { rtt_ms = Timestamp::now() - pending[idx] }` whenever a pending marker existed. Local-clock subtraction only — see `project_per_peer_epoch.md`. | 0–1 channel send per arrival |
//! | 7 | `manager/state.rs::ConnectionStates::recovered_packet` (shared by both `received_data_packet` and `received_parity_packet` recovery branches) | manager | `PacketsRecovered { count }` + `BatchRecovered { latency_ms }` using `SessionFecState::batch_started_at`. Emitted before `fec.evict` so the start timestamp is still readable. | 2 channel sends per recovered batch |
//! | 8 | `manager/state.rs::FecPruneTask::prune` | manager | One `BatchUnrecoverable` per expired tuple with non-empty `missing_positions`. Batches that pruned clean (no holes) were complete from data and are not emitted. | 0–N channel sends per prune tick |
//! | 9 | `manager/state.rs::send_on_session` — interval tick arm | manager | `SendTick { emitted }` per interval (`emitted` is true iff `send_stream_action` returned `Some`). `BatchFillLatency` is **not** emitted — in this protocol every tick ships exactly one full batch, so the metric collapses to `SEND_INTERVAL` and can be derived as `window_ms / send_ticks_busy` post-hoc. | 1 channel send per tick |
//! | 10 | `manager/state.rs::send_on_session` (sender side, after `stream_to`) + `manager/routines/endpoints.rs::request_track` (receiver side, after `streaming_from`); close via `manager/state.rs::ProtocolState::{close_session, close_stream}` | manager | `SessionOpened { local_addr_hash, remote_addr_hash, fec_config }` and `SessionClosed`. Address hashes use keyed SipHash-2-4 with a per-process random key — see `hash_addr`. **This site MUST fire first** for a session; observations before it are dropped (see `entry_mut`). | 1 channel send per |
//! | 11 | `manager/state.rs::ProtocolState::update_address` | manager | `AddressRebind`. One-bit flag flip past the no-op guard. Key rotation is not implemented in this branch and has no observation. | 1 channel send per real rebind |
//! | 12 | Receiver side: `manager/routines/endpoints.rs::send_playback_control_packet` (the app-issued action). Sender side: `manager/state.rs::send_on_session` event-match arms for `Pause`/`Seek`. Both sides emit so each peer's row reflects the event. | manager | `Paused`, `Seeked`. | 1 channel send per event per peer |
//!
//! **Sites intentionally not added** (for maintainability):
//! - No emission inside the FEC module itself, despite it being the natural
//!   place to detect "wasted parity" — that field is left absent on
//!   [`DataEntry`] and the training pipeline can derive it post-hoc as
//!   approximately `parity_packets_received - recovered_batches`. Keeps the
//!   FEC layer free of any data-collection wiring.
//! - No explicit `InterArrival` emission. The collector synthesises it from
//!   the arrival of consecutive `DataPacketReceived` observations using a
//!   per-session `last_arrival_ts` slot in its scratch state.
//! - No explicit `LastActivityGap` emission. Same idea — derived in the
//!   collector at window-close as `window_end - last_arrival_ts`.
//! - No emission inside the transport layer. The packet processor sees every
//!   successfully-decoded packet and has the `session_id`; adding a transport
//!   site would either double-count or only cover undecodable packets, which
//!   aren't usable training rows anyway.
//!
//! Adding a recording point: prefer extending an existing site to emit one
//! more observation kind over creating a new one. Each new site is one more
//! place a future refactor can silently break collection.

use std::collections::VecDeque;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::OnceLock;

use siphasher::sip::SipHasher24;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, interval};
use tracing::instrument;

use crate::manager::packets::{FecConfig, SessionId};
use crate::manager::types::Timestamp;
use crate::prelude::HashMap;

/// Window length over which raw observations are accumulated into a single
/// [`DataEntry`]. Matches `instructions.md` task 1.
pub const WINDOW_MS: u64 = 500;

/// Per-process random key for the address hash. Initialised on first use
/// via `OnceLock` so it's stable for the lifetime of the protocol instance
/// (same `SocketAddr` always hashes to the same value within one run) and
/// unpredictable across runs (a leaked CSV cannot be used as a confirmation
/// oracle by hashing a candidate IP and comparing).
fn addr_hash_key() -> (u64, u64) {
    static KEY: OnceLock<(u64, u64)> = OnceLock::new();
    *KEY.get_or_init(|| (rand::random(), rand::random()))
}

/// Hash a `SocketAddr` into the non-reversible 8-byte tag stored on every
/// [`DataEntry`]. Uses keyed SipHash-2-4 with a per-process random key —
/// satisfies rule 8 (no PII) by making the hash impossible to reproduce or
/// dictionary-attack outside the originating protocol instance. Perf is
/// irrelevant here because this fires once per session lifecycle.
#[must_use]
pub fn hash_addr(addr: &SocketAddr) -> [u8; 8] {
    let (k0, k1) = addr_hash_key();
    let mut h = SipHasher24::new_with_keys(k0, k1);
    h.write(addr.to_string().as_bytes());
    h.finish().to_be_bytes()
}

/// Hash a local port into a [`SessionOpened`]-compatible tag when the bound
/// IP is not directly known (most call sites only have a `Port`). All
/// sessions on the same protocol instance share the same value, which is
/// enough to distinguish runs from different hosts.
///
/// [`SessionOpened`]: Observation::SessionOpened
#[must_use]
pub fn hash_local_port(port: u16) -> [u8; 8] {
    let (k0, k1) = addr_hash_key();
    let mut h = SipHasher24::new_with_keys(k0, k1);
    h.write(&port.to_be_bytes());
    h.finish().to_be_bytes()
}

/// One 500 ms window of accumulated measurements for one session.
///
/// Identity / window-shape fields are always populated; entries only exist
/// after a [`Observation::SessionOpened`] has registered the session. Every
/// metric field is `Option<_>` — `None` means "no observation of this kind
/// landed in this window", which is itself a useful signal.
///
/// Per-direction fields are tagged `_sent` (this side originated the packet)
/// or `_received` (this side observed it inbound). A session is collecting on
/// both peers simultaneously, so a sender-side run will populate the `_sent`
/// columns and a receiver-side run will populate the `_received` columns.
#[derive(Debug, Clone)]
pub struct DataEntry {
    // ============================================================
    // Identity (mandatory — populated at construction)
    // ============================================================
    /// Random `u64` newtype generated during the handshake (see
    /// `manager::packets::SessionId`). The natural join key across rows: one
    /// session = one continuous transfer between two peers. Rows from the
    /// sender's and receiver's collectors share this id, so a post-hoc join
    /// can pair them.
    pub session_id: SessionId,

    /// Local-clock ms (since this peer's `PROTOCOL_EPOCH`) when the entry's
    /// accumulation window opened. Used together with `window_end` to bin
    /// rows and convert raw counters into rates. **Not comparable to a peer's
    /// timestamps** — every process has its own epoch.
    pub window_start: Timestamp,

    /// Local-clock ms when the window was closed, either by the normal 500 ms
    /// tick or by an early drain (session close / `complete()`). The actual
    /// window length is `window_end - window_start`; expect ~500 ms most of
    /// the time, but rows on the tail of a session can be shorter, and the
    /// model should weight by length when computing per-second rates.
    pub window_end: Timestamp,

    /// `xxh3`-truncated 8-byte hash of this peer's bound `SocketAddr`. Stable
    /// for the session's lifetime (set at `SessionOpened`). Non-reversible so
    /// it satisfies rule 8 (no PII). Lets you cluster runs from the same host
    /// pair without storing IPs.
    pub local_addr_hash: [u8; 8],

    /// Same hashing for the peer's `SocketAddr`. Paired with `local_addr_hash`
    /// this gives a stable identifier for the link. If the peer's address
    /// changes mid-session, the hash does **not** update — `address_rebind_seen`
    /// flips instead so the model can isolate windows during the transition.
    pub remote_addr_hash: [u8; 8],

    /// The runtime FEC choice the app made for this stream — scheme
    /// (`Xor` / `ReedSolomon`), `recovery_count`, and `batch_size`. **This is
    /// the regression target's input**: the model learns "given the network
    /// features in this row, what `(scheme, recovery_count)` minimised loss
    /// without wasted parity?". Sticky across the session.
    pub fec_config: FecConfig,

    /// One-shot per-stream latency: local ms from this peer's
    /// `SessionOpened` (request emission, on the receiver side) to the
    /// first `DataPacketReceived` on the same session. Populated as soon as
    /// the first byte lands and then stays sticky on every subsequent
    /// window for the session. `None` on every row of a stream that never
    /// received a data packet (e.g. sender-side rows for an `Output` stream,
    /// or a stream that was rejected). Local clock both ends so no
    /// cross-epoch hazard.
    pub request_to_first_byte_ms: Option<NonZeroU32>,

    // ============================================================
    // Throughput (raw bytes — derive B/s = bytes / (window_end - window_start))
    // ============================================================
    /// Sum of payload bytes this peer emitted on this session during the
    /// window (transport-outbound observation point). `None` = no traffic
    /// went out; meaningfully distinct from `Some(0)`. `NonZero` because the
    /// emitter never posts a 0-byte observation.
    pub bytes_sent: Option<NonZeroU64>,

    /// Sum of payload bytes this peer received on this session during the
    /// window (transport-inbound observation point). Includes data and parity
    /// payload but not protocol headers.
    pub bytes_received: Option<NonZeroU64>,

    // ============================================================
    // Packet counts
    // ============================================================
    /// Data packets emitted by this peer in the window. Combined with
    /// `bytes_sent`, the app can derive average packet size — useful for
    /// detecting tail-of-stream behaviour where batches don't fill.
    pub data_packets_sent: Option<NonZeroU32>,

    /// Data packets received in the window. Subtract `bytes_received` from
    /// `(bytes_received - parity_packets_received * MAX_PAYLOAD_LENGTH)` to
    /// estimate the data-only goodput; or just use this count directly.
    pub data_packets_received: Option<NonZeroU32>,

    /// Parity packets this peer emitted (sender side). Together with
    /// `data_packets_sent` this gives the *realised* parity overhead, which
    /// can differ from the configured ratio when a batch finishes early.
    pub parity_packets_sent: Option<NonZeroU32>,

    /// Parity packets received (receiver side). A high count here paired
    /// with a low `recovered_batches` is a sign of wasted bandwidth.
    pub parity_packets_received: Option<NonZeroU32>,

    // ============================================================
    // Retransmit (three-way split — each is a different loss signal)
    // ============================================================
    /// Receiver side: retransmit requests this peer emitted. Each request
    /// corresponds to one or more chunks the receiver couldn't recover via
    /// FEC. Direct local-loss signal — the receiver knows exactly which
    /// chunks it was missing.
    pub retransmits_issued: Option<NonZeroU32>,

    /// Sender side: retransmit requests received from the peer. This is the
    /// sender's primary visibility into receiver-side loss; it has no other
    /// native way to know which packets failed to arrive.
    pub retransmits_served: Option<NonZeroU32>,

    /// Sender side: data packets actually re-emitted in response to
    /// retransmit requests. May differ from `retransmits_served` if a request
    /// covers chunks the sender no longer holds, or if multiple requests
    /// asked for the same chunk (deduplication).
    pub retransmits_sent: Option<NonZeroU32>,

    // ============================================================
    // FEC efficacy (the columns that directly answer "was the ratio right?")
    // ============================================================
    /// Individual data packets reconstructed by FEC across all recovered
    /// batches in the window. Bigger isn't strictly "better" — it means
    /// recovery was needed; the model should also look at
    /// `unrecoverable_batches` and `wasted_parity` to score the ratio.
    pub recovered_packets: Option<NonZeroU32>,

    /// Batches where FEC recovery succeeded in full. Together with
    /// `unrecoverable_batches` this gives the recovery success rate per
    /// window: `recovered / (recovered + unrecoverable)`.
    pub recovered_batches: Option<NonZeroU32>,

    /// Batches where loss exceeded the chosen `recovery_count`, so FEC
    /// couldn't rebuild the missing data and the receiver had to fall back
    /// to retransmit. **Direct evidence the ratio was too low.** The model
    /// should optimise to keep this column small relative to data volume.
    pub unrecoverable_batches: Option<NonZeroU32>,

    /// Parity packets that arrived for batches already complete from data
    /// alone (i.e. no loss happened in the batch). **Direct evidence the
    /// ratio was too high.** Counterweight to `unrecoverable_batches` — the
    /// model is looking for the sweet spot that minimises both.
    pub wasted_parity: Option<NonZeroU32>,

    // ============================================================
    // Loss characterization (shape, not just rate)
    // ============================================================
    /// Total holes (missing chunks) observed in the window, regardless of
    /// whether they were later filled by FEC or retransmit. The model wants
    /// this together with `data_packets_received` to derive raw loss rate.
    pub holes_observed: Option<NonZeroU32>,

    /// Longest run of consecutive missing chunks seen during the window. A
    /// network with 5 % bursty loss wants very different FEC parameters than
    /// one with 5 % uniform random loss — burstiness is the second-most
    /// important feature after rate. `NonZero` because the emitter only fires
    /// when a burst of length ≥ 1 actually ends.
    pub longest_loss_burst: Option<NonZeroU32>,

    /// Max gap between the highest-seen `batch_pos` and the current one,
    /// taken across all batches active during the window. Useful for
    /// distinguishing real loss from reorder: a packet arriving after others
    /// from a later `batch_pos` was reordered, not lost. `NonZero` because a
    /// distance of 0 is in-order and never emitted.
    pub max_reorder_distance: Option<NonZeroU32>,

    // ============================================================
    // Latency samples (sum + count → app derives mean; sums plain
    // u64 because a single 0 ms sample is legal at ms resolution)
    // ============================================================
    /// Sum of recovery latencies in ms: time from a batch's first packet
    /// arrival to FEC successfully reconstructing the batch. Long tails here
    /// mean recovery is racing the retransmit fallback, suggesting the
    /// configured `recovery_count` is borderline.
    pub recovery_latency_ms_sum: Option<u64>,
    /// Number of samples in `recovery_latency_ms_sum`. Mean = `sum / count`.
    pub recovery_latency_count: Option<NonZeroU32>,

    /// Sender side: sum of "time from first chunk added to a batch to FEC
    /// `sent()` firing" in ms. If this is consistently large, `batch_size`
    /// is too big for the data rate and parity arrives too late to be
    /// useful.
    pub batch_fill_latency_ms_sum: Option<u64>,
    pub batch_fill_latency_count: Option<NonZeroU32>,

    /// Receiver side: sum of RTT samples in ms, measured **entirely in the
    /// local clock** — start time stamped when the retransmit request is
    /// emitted, end time when the requested data arrives. Subtracting a
    /// peer-stamped timestamp would give garbage because each peer has its
    /// own `PROTOCOL_EPOCH` (see `project_per_peer_epoch.md`).
    pub retransmit_rtt_ms_sum: Option<u64>,
    pub retransmit_rtt_count: Option<NonZeroU32>,

    // ============================================================
    // Inter-arrival jitter (receiver side, local clock)
    // ============================================================
    /// Sum of inter-arrival deltas (ms) between consecutive received data
    /// packets. Mean inter-arrival = `sum / count`; combined with throughput
    /// it characterises pacing.
    pub inter_arrival_ms_sum: Option<u64>,
    /// Sum of squared deltas, paired with `_sum` and `_count` to compute
    /// variance without storing every sample:
    /// `var = sumsq / n - (sum / n)^2`. Standard deviation in ms is `sqrt(var)`.
    pub inter_arrival_ms_sumsq: Option<u64>,
    pub inter_arrival_count: Option<NonZeroU32>,

    // ============================================================
    // Application state (latest sample wins within the window)
    // ============================================================
    /// `head` (in bytes) of the receiver's `WriteableBuffer` at the most
    /// recent observation point. Together with `buffer_len` it tells the
    /// model how close the receiver is to underrun — a near-empty buffer
    /// makes the receiver much less tolerant of further loss. Plain `u64`
    /// (no `NonZero`) because 0 is a legitimate "stream just opened" sample.
    pub buffer_head: Option<u64>,

    /// Total length (bytes) of the receiver's buffer. Stable for the
    /// session's lifetime in normal use. `NonZero` because a length of 0
    /// would mean "no buffer wired" — not an interesting observation.
    pub buffer_len: Option<NonZeroU64>,

    /// Wall-clock ms since the last received packet on this session,
    /// sampled when the window closes. A long gap signals stalls that
    /// won't show up in the per-window counters but matter for latency
    /// SLAs. Plain `u32` because a freshly-arrived packet legitimately
    /// samples to 0.
    pub last_activity_gap_ms: Option<u32>,

    // ============================================================
    // Sender pacing (saturation signal)
    // ============================================================
    /// Number of send-loop ticks in the window where data was actually
    /// emitted. Pair with `send_ticks_idle` to compute saturation:
    /// `busy / (busy + idle)`. High saturation means the sender is
    /// network-limited; low saturation means it's app-limited (no data to
    /// send), which is itself important context for interpreting low
    /// throughput.
    pub send_ticks_busy: Option<NonZeroU32>,

    /// Send-loop ticks that found no chunks to emit (stream paused, buffer
    /// drained, or seek pending). Pairs with `send_ticks_busy`.
    pub send_ticks_idle: Option<NonZeroU32>,

    // ============================================================
    // Sticky single-window event flags
    // ============================================================
    /// `true` if a Pause control event landed in this window. Pause periods
    /// produce degenerate counters (no data flow), so the training pipeline
    /// will usually want to drop or weight these rows separately.
    pub paused_during_window: bool,

    /// `true` if a Seek control event landed. Seeks invalidate the
    /// chunk-position monotonicity that `max_reorder_distance` and
    /// `longest_loss_burst` assume; mark the row so the cleansing step can
    /// treat it as a transition period.
    pub seeked_during_window: bool,

    /// `true` if this peer observed the remote's `SocketAddr` change during
    /// the window (NAT rebind / mobility handoff). A window with a rebind
    /// almost always has degraded metrics; useful both as a feature and as
    /// a row-filter flag.
    pub address_rebind_seen: bool,
}

impl DataEntry {
    fn new(session_id: SessionId, identity: SessionIdentity, window_start: Timestamp) -> Self {
        Self {
            session_id,
            window_start,
            window_end: window_start,
            local_addr_hash: identity.local_addr_hash,
            remote_addr_hash: identity.remote_addr_hash,
            fec_config: identity.fec_config,
            request_to_first_byte_ms: identity.request_to_first_byte_ms,

            bytes_sent: None,
            bytes_received: None,
            data_packets_sent: None,
            data_packets_received: None,
            parity_packets_sent: None,
            parity_packets_received: None,
            retransmits_issued: None,
            retransmits_served: None,
            retransmits_sent: None,
            recovered_packets: None,
            recovered_batches: None,
            unrecoverable_batches: None,
            wasted_parity: None,
            holes_observed: None,
            longest_loss_burst: None,
            max_reorder_distance: None,
            recovery_latency_ms_sum: None,
            recovery_latency_count: None,
            batch_fill_latency_ms_sum: None,
            batch_fill_latency_count: None,
            retransmit_rtt_ms_sum: None,
            retransmit_rtt_count: None,
            inter_arrival_ms_sum: None,
            inter_arrival_ms_sumsq: None,
            inter_arrival_count: None,
            buffer_head: None,
            buffer_len: None,
            last_activity_gap_ms: None,
            send_ticks_busy: None,
            send_ticks_idle: None,
            paused_during_window: false,
            seeked_during_window: false,
            address_rebind_seen: false,
        }
    }
}

/// Typed, layer-agnostic event posted to the collector. Each variant maps to
/// one of the collection points discussed in BREAK 1; folding happens in the
/// collector task so emitter sites stay branch-free.
#[derive(Debug, Clone)]
pub enum Observation {
    // ---------- Lifecycle ----------
    /// Bind identity to a session. Must arrive before any other observation
    /// for the session — later observations for unknown sessions are dropped.
    SessionOpened {
        session: SessionId,
        local_addr_hash: [u8; 8],
        remote_addr_hash: [u8; 8],
        fec_config: FecConfig,
    },
    /// Session has closed; flush its open window and stop accepting
    /// observations on this id.
    SessionClosed { session: SessionId },

    // ---------- Throughput ----------
    BytesSent { session: SessionId, bytes: u64 },
    BytesReceived { session: SessionId, bytes: u64 },

    // ---------- Packet counts ----------
    DataPacketSent { session: SessionId },
    DataPacketReceived { session: SessionId },
    ParityPacketSent { session: SessionId },
    ParityPacketReceived { session: SessionId },

    // ---------- Retransmit ----------
    RetransmitIssued { session: SessionId, count: u32 },
    RetransmitServed { session: SessionId, count: u32 },
    RetransmitSent { session: SessionId, count: u32 },

    // ---------- FEC efficacy ----------
    PacketsRecovered { session: SessionId, count: u32 },
    BatchRecovered { session: SessionId, latency_ms: u32 },
    BatchUnrecoverable { session: SessionId },
    WastedParity { session: SessionId, count: u32 },

    // ---------- Loss characterization ----------
    HolesObserved { session: SessionId, count: u32 },
    /// A burst of consecutive lost chunks ended; emitter reports its length.
    LossBurst { session: SessionId, length: u32 },
    ReorderDistance { session: SessionId, distance: u32 },

    // ---------- Latency / jitter samples ----------
    BatchFillLatency { session: SessionId, latency_ms: u32 },
    /// Local-clock RTT sample from retransmit-request emission to arrival.
    /// See memory `project_per_peer_epoch.md` for why this must stay local.
    RetransmitRtt { session: SessionId, rtt_ms: u32 },

    // ---------- Application state (latest wins within a window) ----------
    BufferState {
        session: SessionId,
        head: u64,
        len: u64,
    },
    LastActivityGap { session: SessionId, gap_ms: u32 },

    // ---------- Sender pacing ----------
    /// One iteration of the sender's tick loop. `emitted = true` if data was
    /// produced; otherwise the tick was a no-op (paused, no chunks, etc.).
    SendTick { session: SessionId, emitted: bool },

    // ---------- Sticky single-window event flags ----------
    Paused { session: SessionId },
    Seeked { session: SessionId },
    AddressRebind { session: SessionId },
}

/// Cheap clone-and-fire channel handed to every layer that has something to
/// report. `send` is sync and infallible if the collector is alive (we use
/// `UnboundedSender`); we silently drop if the collector has stopped.
#[derive(Clone, Debug)]
pub struct DataCollectionChannel(mpsc::UnboundedSender<Observation>);

impl DataCollectionChannel {
    /// Post an observation. Never awaits, never blocks. Drops the observation
    /// if the collector has shut down.
    pub fn post(&self, observation: Observation) {
        let _ = self.0.send(observation);
    }
}

enum DrainRequest {
    Session(SessionId, oneshot::Sender<Vec<DataEntry>>),
    All(oneshot::Sender<HashMap<SessionId, Vec<DataEntry>>>),
}

/// Public handle the app uses to pull accumulated entries.
#[derive(Clone, Debug)]
pub struct DrainHandle(mpsc::Sender<DrainRequest>);

impl DrainHandle {
    /// Take ownership of every completed entry collected for `session` since
    /// the last drain. The session's open window is also flushed so callers
    /// see partial progress promptly. Returns an empty Vec on unknown sessions
    /// and on collector shutdown.
    pub async fn drain(&self, session: SessionId) -> Vec<DataEntry> {
        let (tx, rx) = oneshot::channel();
        if self.0.send(DrainRequest::Session(session, tx)).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }

    /// Drain every session at once. Used by stream `complete()` to ship the
    /// trailing partial windows alongside the final accumulated entries.
    pub async fn drain_all(&self) -> HashMap<SessionId, Vec<DataEntry>> {
        let (tx, rx) = oneshot::channel();
        if self.0.send(DrainRequest::All(tx)).await.is_err() {
            return HashMap::default();
        }
        rx.await.unwrap_or_default()
    }
}

/// Spawn the single collector task. Returns the post-side channel (cloned and
/// distributed to layers) and the drain-side handle (stored for the app).
///
/// Uses raw `tokio::spawn` rather than `HandleMonitor::dispatch` because the
/// collector is a long-lived daemon, not a fire-and-forget unit of work:
/// putting it on the shared monitor would cause `flush()` (used during
/// shutdown coordination) to wait forever. The task exits cleanly when every
/// `DataCollectionChannel` clone is dropped (i.e. when `ProtocolState` is
/// torn down).
#[must_use]
pub fn start_data_collector() -> (DataCollectionChannel, DrainHandle) {
    let (obs_tx, obs_rx) = mpsc::unbounded_channel::<Observation>();
    let (drain_tx, drain_rx) = mpsc::channel::<DrainRequest>(32);

    tokio::spawn(collector_loop(obs_rx, drain_rx));

    (DataCollectionChannel(obs_tx), DrainHandle(drain_tx))
}

#[instrument(skip_all)]
async fn collector_loop(
    mut obs_rx: mpsc::UnboundedReceiver<Observation>,
    mut drain_rx: mpsc::Receiver<DrainRequest>,
) {
    let mut state = CollectorState::default();
    let mut tick = interval(Duration::from_millis(WINDOW_MS));
    // Skip the immediate first tick — windows should be window-length long,
    // not however-many-ms-after-startup-we-are long.
    tick.tick().await;

    loop {
        tokio::select! {
            obs = obs_rx.recv() => {
                match obs {
                    Some(observation) => state.fold(observation),
                    None => break,
                }
            }
            _ = tick.tick() => state.rotate(),
            req = drain_rx.recv() => {
                match req {
                    Some(DrainRequest::Session(s, tx)) => {
                        let _ = tx.send(state.drain_session(s));
                    }
                    Some(DrainRequest::All(tx)) => {
                        let _ = tx.send(state.drain_all());
                    }
                    None => {}
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct SessionIdentity {
    local_addr_hash: [u8; 8],
    remote_addr_hash: [u8; 8],
    fec_config: FecConfig,
    /// Local ms stamp captured when the session's `SessionOpened` was folded.
    /// Used to derive `request_to_first_byte_ms` once data starts arriving.
    opened_at: Timestamp,
    /// Sticky once the first `DataPacketReceived` lands. Copied onto every
    /// new window for the session.
    request_to_first_byte_ms: Option<NonZeroU32>,
}

#[derive(Default)]
struct CollectorState {
    /// Open windows keyed by session.
    current: HashMap<SessionId, DataEntry>,
    /// Sticky identity copied onto every new window for the session.
    identity: HashMap<SessionId, SessionIdentity>,
    /// Closed windows awaiting drain.
    completed: HashMap<SessionId, VecDeque<DataEntry>>,
    /// Local-clock timestamp of the most recent `DataPacketReceived` per
    /// session. Used to synthesise `InterArrival` deltas without forcing
    /// emitters to stamp the clock (keeps site 2 to one type-check + two
    /// channel sends), and to compute `last_activity_gap_ms` at window close.
    last_arrival: HashMap<SessionId, Timestamp>,
}

/// Helper to fold an additive counter into a plain `Option<T>` field.
fn add_opt<T>(slot: &mut Option<T>, delta: T)
where
    T: core::ops::Add<Output = T> + Copy + Default,
{
    *slot = Some(slot.unwrap_or_default() + delta);
}

/// Add `delta` into a `NonZeroU32` slot. No-op on `delta == 0`, which keeps
/// the niche invariant intact and lets callers be sloppy about zero events.
fn bump_nz32(slot: &mut Option<NonZeroU32>, delta: u32) {
    if delta == 0 {
        return;
    }
    let total = slot.map_or(0u32, NonZeroU32::get).saturating_add(delta);
    // total > 0 because delta > 0, so `new` is always `Some`.
    *slot = NonZeroU32::new(total);
}

/// Add `delta` into a `NonZeroU64` slot. Same semantics as [`bump_nz32`].
fn bump_nz64(slot: &mut Option<NonZeroU64>, delta: u64) {
    if delta == 0 {
        return;
    }
    let total = slot.map_or(0u64, NonZeroU64::get).saturating_add(delta);
    *slot = NonZeroU64::new(total);
}

/// Keep the running max in a `NonZeroU32` slot. Zero candidates are dropped.
fn max_nz32(slot: &mut Option<NonZeroU32>, candidate: u32) {
    let Some(nz) = NonZeroU32::new(candidate) else {
        return;
    };
    *slot = Some(slot.map_or(nz, |cur| cur.max(nz)));
}

/// Set a `NonZeroU64` slot to the latest non-zero sample.
fn latest_nz64(slot: &mut Option<NonZeroU64>, sample: u64) {
    if let Some(nz) = NonZeroU64::new(sample) {
        *slot = Some(nz);
    }
}

impl CollectorState {
    /// Open-window accessor that requires registered identity. Returns `None`
    /// for sessions that haven't seen a `SessionOpened` yet — observations on
    /// such sessions are dropped, since we can't build a complete entry
    /// without the identity fields and they're guaranteed to be present.
    fn entry_mut(&mut self, session: SessionId) -> Option<&mut DataEntry> {
        let identity = *self.identity.get(&session)?;
        Some(
            self.current
                .entry(session)
                .or_insert_with(|| DataEntry::new(session, identity, Timestamp::now())),
        )
    }

    // `clippy::needless_pass_by_value`: every arm destructures the variant by
    // value (no shared `&Observation` would work without per-arm `*deref`s).
    // Taking it by reference would force the match to clone fields just to use
    // them.
    // `clippy::too_many_lines`: this is the dispatch table for every
    // Observation variant — its size is structural, not complexity. Adding a
    // new variant adds one arm here; splitting would mean splitting the enum.
    #[allow(clippy::needless_pass_by_value, clippy::too_many_lines)]
    fn fold(&mut self, obs: Observation) {
        match obs {
            Observation::SessionOpened {
                session,
                local_addr_hash,
                remote_addr_hash,
                fec_config,
            } => {
                let identity = SessionIdentity {
                    local_addr_hash,
                    remote_addr_hash,
                    fec_config,
                    opened_at: Timestamp::now(),
                    request_to_first_byte_ms: None,
                };
                self.identity.insert(session, identity);
                // Eagerly open a window so callers see the entry even when no
                // metrics land before the first rotate.
                self.current
                    .entry(session)
                    .or_insert_with(|| DataEntry::new(session, identity, Timestamp::now()));
            }
            Observation::SessionClosed { session } => {
                self.flush_open(session);
                self.identity.remove(&session);
                self.last_arrival.remove(&session);
            }

            Observation::BytesSent { session, bytes } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz64(&mut e.bytes_sent, bytes);
                }
            }
            Observation::BytesReceived { session, bytes } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz64(&mut e.bytes_received, bytes);
                }
            }

            Observation::DataPacketSent { session } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.data_packets_sent, 1);
                }
            }
            Observation::DataPacketReceived { session } => {
                let now = Timestamp::now();
                let prev = self.last_arrival.insert(session, now);
                // First arrival on this session — derive request-to-first-byte
                // from the locally-stamped `opened_at` and stick it on
                // identity so every subsequent window inherits it.
                if prev.is_none()
                    && let Some(identity) = self.identity.get_mut(&session)
                    && identity.request_to_first_byte_ms.is_none()
                {
                    let delta = now.get().saturating_sub(identity.opened_at.get());
                    let ms = u32::try_from(delta).unwrap_or(u32::MAX);
                    identity.request_to_first_byte_ms = NonZeroU32::new(ms);
                }
                // Snapshot for the current entry update below, since
                // `entry_mut` borrows self mutably and we can't keep
                // `identity` alive across that call.
                let rtfb = self
                    .identity
                    .get(&session)
                    .and_then(|i| i.request_to_first_byte_ms);
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.data_packets_received, 1);
                    // Backfill on the open window too — without this, the
                    // first window's row would have `None` even though the
                    // first byte landed inside it.
                    if e.request_to_first_byte_ms.is_none() {
                        e.request_to_first_byte_ms = rtfb;
                    }
                    // Synthesise inter-arrival from consecutive arrivals.
                    // Skip the first arrival of each session (no previous
                    // sample) and saturate at u32::MAX in the pathological
                    // case of a multi-day gap.
                    if let Some(prev_ts) = prev {
                        let dt = now.get().saturating_sub(prev_ts.get());
                        let d = u32::try_from(dt).unwrap_or(u32::MAX);
                        let d64 = u64::from(d);
                        add_opt(&mut e.inter_arrival_ms_sum, d64);
                        add_opt(&mut e.inter_arrival_ms_sumsq, d64 * d64);
                        bump_nz32(&mut e.inter_arrival_count, 1);
                    }
                }
            }
            Observation::ParityPacketSent { session } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.parity_packets_sent, 1);
                }
            }
            Observation::ParityPacketReceived { session } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.parity_packets_received, 1);
                }
            }

            Observation::RetransmitIssued { session, count } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.retransmits_issued, count);
                }
            }
            Observation::RetransmitServed { session, count } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.retransmits_served, count);
                }
            }
            Observation::RetransmitSent { session, count } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.retransmits_sent, count);
                }
            }

            Observation::PacketsRecovered { session, count } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.recovered_packets, count);
                }
            }
            Observation::BatchRecovered {
                session,
                latency_ms,
            } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.recovered_batches, 1);
                    add_opt(&mut e.recovery_latency_ms_sum, u64::from(latency_ms));
                    bump_nz32(&mut e.recovery_latency_count, 1);
                }
            }
            Observation::BatchUnrecoverable { session } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.unrecoverable_batches, 1);
                }
            }
            Observation::WastedParity { session, count } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.wasted_parity, count);
                }
            }

            Observation::HolesObserved { session, count } => {
                if let Some(e) = self.entry_mut(session) {
                    bump_nz32(&mut e.holes_observed, count);
                }
            }
            Observation::LossBurst { session, length } => {
                if let Some(e) = self.entry_mut(session) {
                    max_nz32(&mut e.longest_loss_burst, length);
                }
            }
            Observation::ReorderDistance { session, distance } => {
                if let Some(e) = self.entry_mut(session) {
                    max_nz32(&mut e.max_reorder_distance, distance);
                }
            }

            Observation::BatchFillLatency {
                session,
                latency_ms,
            } => {
                if let Some(e) = self.entry_mut(session) {
                    add_opt(&mut e.batch_fill_latency_ms_sum, u64::from(latency_ms));
                    bump_nz32(&mut e.batch_fill_latency_count, 1);
                }
            }
            Observation::RetransmitRtt { session, rtt_ms } => {
                if let Some(e) = self.entry_mut(session) {
                    add_opt(&mut e.retransmit_rtt_ms_sum, u64::from(rtt_ms));
                    bump_nz32(&mut e.retransmit_rtt_count, 1);
                }
            }

            Observation::BufferState {
                session,
                head,
                len,
            } => {
                if let Some(e) = self.entry_mut(session) {
                    // Latest snapshot wins; emitters fire on state changes.
                    e.buffer_head = Some(head);
                    latest_nz64(&mut e.buffer_len, len);
                }
            }
            Observation::LastActivityGap { session, gap_ms } => {
                if let Some(e) = self.entry_mut(session) {
                    e.last_activity_gap_ms = Some(gap_ms);
                }
            }

            Observation::SendTick { session, emitted } => {
                if let Some(e) = self.entry_mut(session) {
                    if emitted {
                        bump_nz32(&mut e.send_ticks_busy, 1);
                    } else {
                        bump_nz32(&mut e.send_ticks_idle, 1);
                    }
                }
            }

            Observation::Paused { session } => {
                if let Some(e) = self.entry_mut(session) {
                    e.paused_during_window = true;
                }
            }
            Observation::Seeked { session } => {
                if let Some(e) = self.entry_mut(session) {
                    e.seeked_during_window = true;
                }
            }
            Observation::AddressRebind { session } => {
                if let Some(e) = self.entry_mut(session) {
                    e.address_rebind_seen = true;
                }
            }
        }
    }

    /// Close the open window for every session, push it onto the completed
    /// queue, and open a fresh window. Sticky identity carries over so the
    /// next window's `fec_config`/addr fields stay populated even if no
    /// observation lands in that window.
    fn rotate(&mut self) {
        let now = Timestamp::now();
        let sessions: Vec<SessionId> = self.current.keys().copied().collect();
        for session in sessions {
            self.close_window(session, now);
        }
    }

    fn close_window(&mut self, session: SessionId, now: Timestamp) {
        let Some(mut entry) = self.current.remove(&session) else {
            return;
        };
        entry.window_end = now;
        // Derive last_activity_gap_ms from the per-session last-arrival
        // tracker — no emitter has to stamp this at site 2 directly.
        if let Some(last) = self.last_arrival.get(&session) {
            let gap = now.get().saturating_sub(last.get());
            entry.last_activity_gap_ms = Some(u32::try_from(gap).unwrap_or(u32::MAX));
        }
        self.completed.entry(session).or_default().push_back(entry);
    }

    fn flush_open(&mut self, session: SessionId) {
        let now = Timestamp::now();
        self.close_window(session, now);
    }

    fn drain_session(&mut self, session: SessionId) -> Vec<DataEntry> {
        self.flush_open(session);
        self.completed
            .remove(&session)
            .map(|q| q.into_iter().collect())
            .unwrap_or_default()
    }

    fn drain_all(&mut self) -> HashMap<SessionId, Vec<DataEntry>> {
        let sessions: Vec<SessionId> = self.current.keys().copied().collect();
        for session in sessions {
            self.flush_open(session);
        }
        let drained: HashMap<SessionId, Vec<DataEntry>> = self
            .completed
            .drain()
            .map(|(s, q)| (s, q.into_iter().collect()))
            .collect();
        drained
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::packets::FecScheme;
    use crate::prelude::PROTOCOL_EPOCH;
    use tokio::time::Instant;

    fn ensure_epoch() {
        let _ = PROTOCOL_EPOCH.set(Instant::now());
    }

    fn fec() -> FecConfig {
        FecConfig {
            scheme: FecScheme::Xor,
            recovery_count: 0,
            batch_size: 28,
        }
    }

    // `clippy::too_many_lines`: this test exhaustively exercises every
    // Observation variant against every field, by design. Splitting it would
    // duplicate setup without improving coverage.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn drain_folds_every_observation_kind() {
        ensure_epoch();
        let (chan, drain) = start_data_collector();
        let s = SessionId::new(42);

        chan.post(Observation::SessionOpened {
            session: s,
            local_addr_hash: [1; 8],
            remote_addr_hash: [2; 8],
            fec_config: fec(),
        });
        // Ensure a non-zero delta to `DataPacketReceived` so the
        // request-to-first-byte field lands as `Some(NonZero)`. A 0 ms
        // delta would map to `None` through `NonZeroU32::new`.
        tokio::time::sleep(Duration::from_millis(5)).await;
        // throughput + counts
        chan.post(Observation::BytesSent {
            session: s,
            bytes: 1500,
        });
        chan.post(Observation::BytesReceived {
            session: s,
            bytes: 700,
        });
        chan.post(Observation::DataPacketSent { session: s });
        chan.post(Observation::DataPacketReceived { session: s });
        chan.post(Observation::DataPacketReceived { session: s });
        chan.post(Observation::ParityPacketSent { session: s });
        chan.post(Observation::ParityPacketReceived { session: s });
        // retransmits
        chan.post(Observation::RetransmitIssued {
            session: s,
            count: 2,
        });
        chan.post(Observation::RetransmitServed {
            session: s,
            count: 1,
        });
        chan.post(Observation::RetransmitSent {
            session: s,
            count: 1,
        });
        // FEC efficacy
        chan.post(Observation::PacketsRecovered {
            session: s,
            count: 3,
        });
        chan.post(Observation::BatchRecovered {
            session: s,
            latency_ms: 12,
        });
        chan.post(Observation::BatchRecovered {
            session: s,
            latency_ms: 8,
        });
        chan.post(Observation::BatchUnrecoverable { session: s });
        chan.post(Observation::WastedParity {
            session: s,
            count: 1,
        });
        // loss
        chan.post(Observation::HolesObserved {
            session: s,
            count: 4,
        });
        chan.post(Observation::LossBurst {
            session: s,
            length: 3,
        });
        chan.post(Observation::LossBurst {
            session: s,
            length: 5,
        });
        chan.post(Observation::ReorderDistance {
            session: s,
            distance: 2,
        });
        chan.post(Observation::ReorderDistance {
            session: s,
            distance: 7,
        });
        // latency samples — inter-arrival is synthesised in the collector
        // from `DataPacketReceived` arrival times, not posted explicitly.
        chan.post(Observation::BatchFillLatency {
            session: s,
            latency_ms: 20,
        });
        chan.post(Observation::RetransmitRtt {
            session: s,
            rtt_ms: 30,
        });
        // app state
        chan.post(Observation::BufferState {
            session: s,
            head: 1024,
            len: 4096,
        });
        chan.post(Observation::LastActivityGap {
            session: s,
            gap_ms: 50,
        });
        // pacing
        chan.post(Observation::SendTick {
            session: s,
            emitted: true,
        });
        chan.post(Observation::SendTick {
            session: s,
            emitted: false,
        });
        chan.post(Observation::SendTick {
            session: s,
            emitted: true,
        });
        // flags
        chan.post(Observation::Paused { session: s });
        chan.post(Observation::Seeked { session: s });

        tokio::time::sleep(Duration::from_millis(20)).await;

        let entries = drain.drain(s).await;
        assert_eq!(entries.len(), 1);
        let e = &entries[0];

        // identity
        assert_eq!(e.session_id, s);
        assert_eq!(e.local_addr_hash, [1; 8]);
        assert_eq!(e.remote_addr_hash, [2; 8]);
        assert_eq!(e.fec_config.scheme, FecScheme::Xor);
        // First DataPacketReceived landed near-immediately after
        // SessionOpened in this test → should be populated (probably 0 ms,
        // we just assert presence).
        assert!(e.request_to_first_byte_ms.is_some());

        // counters (NonZero-backed)
        assert_eq!(e.bytes_sent.map(NonZeroU64::get), Some(1500));
        assert_eq!(e.bytes_received.map(NonZeroU64::get), Some(700));
        assert_eq!(e.data_packets_sent.map(NonZeroU32::get), Some(1));
        assert_eq!(e.data_packets_received.map(NonZeroU32::get), Some(2));
        assert_eq!(e.parity_packets_sent.map(NonZeroU32::get), Some(1));
        assert_eq!(e.parity_packets_received.map(NonZeroU32::get), Some(1));
        assert_eq!(e.retransmits_issued.map(NonZeroU32::get), Some(2));
        assert_eq!(e.retransmits_served.map(NonZeroU32::get), Some(1));
        assert_eq!(e.retransmits_sent.map(NonZeroU32::get), Some(1));
        assert_eq!(e.recovered_packets.map(NonZeroU32::get), Some(3));
        assert_eq!(e.recovered_batches.map(NonZeroU32::get), Some(2));
        assert_eq!(e.unrecoverable_batches.map(NonZeroU32::get), Some(1));
        assert_eq!(e.wasted_parity.map(NonZeroU32::get), Some(1));
        assert_eq!(e.holes_observed.map(NonZeroU32::get), Some(4));

        // max / latency
        assert_eq!(e.longest_loss_burst.map(NonZeroU32::get), Some(5));
        assert_eq!(e.max_reorder_distance.map(NonZeroU32::get), Some(7));
        assert_eq!(e.recovery_latency_ms_sum, Some(20));
        assert_eq!(e.recovery_latency_count.map(NonZeroU32::get), Some(2));
        assert_eq!(e.batch_fill_latency_ms_sum, Some(20));
        assert_eq!(e.batch_fill_latency_count.map(NonZeroU32::get), Some(1));
        assert_eq!(e.retransmit_rtt_ms_sum, Some(30));
        assert_eq!(e.retransmit_rtt_count.map(NonZeroU32::get), Some(1));
        // Synthesised by the collector from the two `DataPacketReceived`
        // posts — the first arrival has no previous sample, the second
        // produces a delta of ~0ms because the posts are back-to-back.
        assert_eq!(e.inter_arrival_count.map(NonZeroU32::get), Some(1));
        assert!(e.inter_arrival_ms_sum.is_some());
        assert!(e.inter_arrival_ms_sumsq.is_some());

        // app state
        assert_eq!(e.buffer_head, Some(1024));
        assert_eq!(e.buffer_len.map(NonZeroU64::get), Some(4096));
        // The explicit LastActivityGap post sets it; on drain the collector
        // also overwrites with (window_end - last_arrival), but both should
        // be small and `Some`.
        assert!(e.last_activity_gap_ms.is_some());

        // pacing
        assert_eq!(e.send_ticks_busy.map(NonZeroU32::get), Some(2));
        assert_eq!(e.send_ticks_idle.map(NonZeroU32::get), Some(1));

        // flags
        assert!(e.paused_during_window);
        assert!(e.seeked_during_window);
        assert!(!e.address_rebind_seen);
    }

    #[tokio::test]
    async fn observations_before_session_opened_are_dropped() {
        ensure_epoch();
        let (chan, drain) = start_data_collector();
        let s = SessionId::new(99);

        chan.post(Observation::DataPacketReceived { session: s });
        chan.post(Observation::BytesReceived {
            session: s,
            bytes: 1000,
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(drain.drain(s).await.is_empty());
    }
}
