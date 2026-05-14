//! Receiver-only wrapper around the core API for offline data-collection
//! sweeps. Drives the regression-training pipeline: pick a list of peer
//! addresses, a list of [`FecConfig`]s to test, and the wrapper will request
//! a freshly-generated PRG stream from each combination, verify it on
//! arrival, and stream [`DataEntry`] rows out through an
//! [`UnboundedReceiver`].
//!
//! The wrapper deliberately exposes a tiny surface:
//!
//! - [`Receiver::open`]    — construct a protocol instance (parallelism level
//!   set here for the lifetime of the receiver — the bound is normally a
//!   hardware property of the host, not per-call).
//! - [`Receiver::sweep`]   — fire-and-forget: iterate `configs × targets`
//!   ([config-outer, sliding-window over targets](self#sweep-shape)) and
//!   emit rows on the returned channel.
//! - [`Receiver::run_once`] — caller-driven single transfer.
//!
//! No track-id parameter exists in the public API — the wrapper *is* the
//! responder protocol on the receiver side, and the track-id wire format is
//! internal. See [`encode_track_id`] / [`verify_prg`] for the layout; when
//! the server-side wrapper lands these will be lifted into a shared module.
//!
//! [`UnboundedReceiver`]: tokio::sync::mpsc::UnboundedReceiver
//! [`DataEntry`]: crate::utils::DataEntry

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::api::WriteableBuffer;
use crate::api::core::{Api, Connection};
use crate::api::dc_proto::{TRACK_ID_LEN, encode_track_id, verify_prg};
use crate::api::types::{
    Connection as ConnectionTrait, PendingConnection as PendingConnectionTrait,
    PendingStream as PendingStreamTrait, Stream as StreamTrait,
};
use crate::error::ApiErrors;
use crate::manager::packets::FecConfig;
use crate::prelude::HashMap;
use crate::utils::DataEntry;

/// Hard cap on per-transfer buffer size. The wire-format `length` is a `u32`
/// (≈4 GB max) but holding multi-GB buffers in memory for a sweep is
/// nonsense for the use case (music-streaming-scale transfers); cap at
/// 256 MiB and let callers narrow it further if they want.
pub const MAX_BUFFER_LEN: usize = 256 * 1024 * 1024;

/// How often the sweep task drains in-flight entries from the active stream.
/// Smaller intervals push more rows through the channel sooner at higher
/// channel-send cost; larger intervals buffer rows in the collector longer.
/// 250 ms means at most 250 ms of latency between an entry being closed by
/// the collector's 500 ms rotate and reaching the caller.
const DRAIN_INTERVAL: Duration = Duration::from_millis(250);

/// Shared connection cache used by parallel sweep tasks.
type ConnCache = Arc<Mutex<HashMap<SocketAddr, Connection>>>;

/// Receiver-only client. Holds a single `Api` instance shared across runs;
/// connections opened during a sweep are cached per target so subsequent
/// runs against the same peer skip the handshake cost (with transparent
/// reopen on failure).
pub struct Receiver {
    api: Arc<Api>,
    parallelism: usize,
}

impl Receiver {
    /// Open the protocol with a fixed parallelism level used by [`sweep`].
    /// The level applies for the lifetime of this receiver — to change it,
    /// drop this instance and open a new one.
    ///
    /// # Errors
    /// - Anything [`Api::open`] returns (invalid app id, invalid port,
    ///   already-open).
    /// - [`ApiErrors::InvalidPort`] re-used as a stand-in if `parallelism`
    ///   is `0` — there's no dedicated error variant and a zero sliding
    ///   window would never make progress.
    ///
    /// [`sweep`]: Self::sweep
    pub fn open(
        app_id: &str,
        port: Option<u16>,
        parallelism: usize,
    ) -> Result<Self, ApiErrors> {
        if parallelism == 0 {
            return Err(ApiErrors::InvalidPort);
        }
        Ok(Self {
            api: Arc::new(Api::open(app_id, port)?),
            parallelism,
        })
    }

    /// Run a `configs × targets` sweep in a spawned task. Returns an
    /// unbounded receiver that yields every [`DataEntry`] the collector
    /// produces during the sweep, including the final flush. The channel
    /// closes when the sweep finishes — drain to completion.
    ///
    /// <a id="sweep-shape"></a>
    /// **Shape**: each config in `configs` runs sequentially; within a
    /// config, transfers against `targets` execute on a sliding window of
    /// size `parallelism` (set at [`open`]). A target finishing immediately
    /// frees its slot, so the window is rate-limited by the wrapper, not
    /// pinned to the slowest target in any sub-chunk. A failed transfer
    /// (peer unreachable, request rejected, transfer stalled past
    /// `per_run_timeout`, or PRG verification mismatch on arrival) is
    /// logged at WARN and skipped; the sweep continues to the next pair.
    ///
    /// # Errors
    /// - [`ApiErrors::BufferTooLarge`] if `buffer_len` is `0` or exceeds
    ///   [`MAX_BUFFER_LEN`].
    ///
    /// [`open`]: Self::open
    pub fn sweep(
        &self,
        targets: Vec<SocketAddr>,
        configs: Vec<FecConfig>,
        buffer_len: usize,
        per_run_timeout: Duration,
    ) -> Result<mpsc::UnboundedReceiver<DataEntry>, ApiErrors> {
        validate_buffer_len(buffer_len)?;
        let (tx, rx) = mpsc::unbounded_channel();
        let api = self.api.clone();
        let parallelism = self.parallelism;
        tokio::spawn(async move {
            run_sweep(
                api,
                targets,
                configs,
                buffer_len,
                per_run_timeout,
                parallelism,
                tx,
            )
            .await;
        });
        Ok(rx)
    }

    /// Run a single transfer against one target with one [`FecConfig`].
    /// Returns the entries produced by that run only. Useful when the
    /// caller wants to drive scheduling itself.
    ///
    /// # Errors
    /// - [`ApiErrors::BufferTooLarge`] for an out-of-range `buffer_len`.
    /// - Any failure inside the transfer is logged and the returned vec is
    ///   empty.
    pub async fn run_once(
        &self,
        target: SocketAddr,
        config: FecConfig,
        buffer_len: usize,
        per_run_timeout: Duration,
    ) -> Result<Vec<DataEntry>, ApiErrors> {
        validate_buffer_len(buffer_len)?;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut connection: Option<Connection> = None;
        let returned = execute_transfer(
            &self.api,
            target,
            &mut connection,
            config,
            buffer_len,
            per_run_timeout,
            &tx,
        )
        .await;
        drop(returned);
        drop(tx);
        let mut entries = Vec::new();
        while let Some(e) = rx.recv().await {
            entries.push(e);
        }
        Ok(entries)
    }
}

fn validate_buffer_len(buffer_len: usize) -> Result<(), ApiErrors> {
    if buffer_len == 0 || buffer_len > MAX_BUFFER_LEN {
        return Err(ApiErrors::BufferTooLarge);
    }
    Ok(())
}

async fn run_sweep(
    api: Arc<Api>,
    targets: Vec<SocketAddr>,
    configs: Vec<FecConfig>,
    buffer_len: usize,
    per_run_timeout: Duration,
    parallelism: usize,
    tx: mpsc::UnboundedSender<DataEntry>,
) {
    let cache: ConnCache = Arc::new(Mutex::new(HashMap::default()));

    for config in configs {
        // Sliding window of `parallelism` in-flight transfers for this
        // config. As one finishes, the next target slot starts immediately
        // — slower targets in the window do not block faster ones.
        let mut set: JoinSet<()> = JoinSet::new();
        let mut iter = targets.iter().copied();

        for _ in 0..parallelism {
            if let Some(target) = iter.next() {
                spawn_target(
                    &mut set,
                    api.clone(),
                    cache.clone(),
                    tx.clone(),
                    target,
                    config,
                    buffer_len,
                    per_run_timeout,
                );
            } else {
                break;
            }
        }

        while let Some(joined) = set.join_next().await {
            if let Err(e) = joined {
                // A panicking transfer task is a bug, not a peer failure.
                warn!("dc_receiver: transfer task panicked: {e:?}");
            }
            if let Some(target) = iter.next() {
                spawn_target(
                    &mut set,
                    api.clone(),
                    cache.clone(),
                    tx.clone(),
                    target,
                    config,
                    buffer_len,
                    per_run_timeout,
                );
            }
        }
    }
    // tx drops here → channel closes → caller's loop exits cleanly.
}

#[allow(clippy::too_many_arguments)]
// All seven values are independent inputs to a self-contained task; bundling
// them into a struct would just add ceremony at the call sites without
// hiding any cross-coupling.
fn spawn_target(
    set: &mut JoinSet<()>,
    api: Arc<Api>,
    cache: ConnCache,
    tx: mpsc::UnboundedSender<DataEntry>,
    target: SocketAddr,
    config: FecConfig,
    buffer_len: usize,
    per_run_timeout: Duration,
) {
    set.spawn(async move {
        let mut conn_slot = cache.lock().await.remove(&target);
        let returned = execute_transfer(
            &api,
            target,
            &mut conn_slot,
            config,
            buffer_len,
            per_run_timeout,
            &tx,
        )
        .await;
        if let Some(c) = returned {
            cache.lock().await.insert(target, c);
        }
    });
}

/// One transfer through the pipeline. On success returns the alive
/// connection so the cache (or a one-shot caller) can keep it; on failure
/// returns `None` (already logged).
///
/// The `connection` slot lets the caller hand in a previously-cached
/// connection — if `Some`, we reuse it; if reuse fails we drop it and try
/// one reopen before giving up.
#[allow(clippy::too_many_arguments)]
// Same rationale as `spawn_target`: all parameters are independent inputs.
async fn execute_transfer(
    api: &Arc<Api>,
    target: SocketAddr,
    connection: &mut Option<Connection>,
    config: FecConfig,
    buffer_len: usize,
    per_run_timeout: Duration,
    tx: &mpsc::UnboundedSender<DataEntry>,
) -> Option<Connection> {
    if let Some(c) = connection.take() {
        if let Some(result) = try_run(c, config, buffer_len, per_run_timeout, tx, true).await {
            return Some(result);
        }
        debug!("dc_receiver: cached connection to {target} died — reopening once");
    }
    let fresh = connect(api, target, per_run_timeout).await?;
    try_run(fresh, config, buffer_len, per_run_timeout, tx, false).await
}

/// Connect + handshake, bounded by `per_run_timeout`. Without the bound the
/// underlying `Api::connect` would retry until the protocol-internal
/// handshake budget runs out, which can be much longer than the per-run
/// SLO a sweep caller picks.
async fn connect(
    api: &Arc<Api>,
    target: SocketAddr,
    per_run_timeout: Duration,
) -> Option<Connection> {
    let fut = async {
        let pending = api
            .connect(target)
            .await
            .inspect_err(|e| warn!("dc_receiver: connect to {target} failed: {e:?}"))
            .ok()?;
        pending
            .ready()
            .await
            .inspect_err(|e| warn!("dc_receiver: handshake with {target} failed: {e:?}"))
            .ok()
    };
    if let Ok(opt) = timeout(per_run_timeout, fut).await {
        opt
    } else {
        warn!("dc_receiver: connect to {target} timed out after {per_run_timeout:?}");
        None
    }
}

/// The actual transfer. `cached` is true if `conn` came from the per-target
/// cache (so failures here trigger the one-shot reopen in `execute_transfer`).
async fn try_run(
    conn: Connection,
    config: FecConfig,
    buffer_len: usize,
    per_run_timeout: Duration,
    tx: &mpsc::UnboundedSender<DataEntry>,
    cached: bool,
) -> Option<Connection> {
    let seed: u64 = rand::random();
    #[allow(clippy::cast_possible_truncation)] // bounded by MAX_BUFFER_LEN ≪ u32::MAX
    let length = buffer_len as u32;
    let track_id = encode_track_id(seed, length);

    // SAFETY: We allocate a fresh `Box<[u8]>`, take its raw pointer, hand
    // ownership to the protocol via `WriteableBuffer`, and reclaim exactly
    // once after the stream resolves. No other code path holds the pointer
    // during that window. The integration suite uses the same pattern
    // (`Box::into_raw` / `Box::from_raw`). `SendableRaw` is a manual `Send`
    // adapter because raw `*mut [u8]` isn't `Send`; the spawned task only
    // dereferences after the protocol's done with it (or never, on failure).
    #[allow(unsafe_code)]
    let raw = SendableRaw(Box::into_raw(vec![0u8; buffer_len].into_boxed_slice()));
    let buffer = WriteableBuffer::from(raw.0);

    let result = transfer(conn, track_id, buffer, config, per_run_timeout, tx).await;

    match result {
        Ok(conn) => {
            // SAFETY: stream resolved successfully → protocol is done with
            // the pointer; reclaim and verify.
            #[allow(unsafe_code)]
            let received = unsafe { Box::from_raw(raw.0) };
            if !verify_prg(seed, &received) {
                warn!("dc_receiver: PRG mismatch on completed transfer (seed={seed:#x})");
            }
            Some(conn)
        }
        Err(conn_back) => {
            // SAFETY: even on failure the protocol no longer holds the
            // pointer once the Stream/PendingStream is dropped — both
            // ownership paths went through the `WriteableBuffer` which is
            // a field on Stream / StreamingFrom, both consumed by the time
            // we get here.
            #[allow(unsafe_code)]
            let _ = unsafe { Box::from_raw(raw.0) };
            if cached {
                // Signal "try a reopen" to the caller.
                None
            } else {
                conn_back
            }
        }
    }
}

/// Wrapper to carry a `*mut [u8]` across an `await` inside a `tokio::spawn`
/// task. The pointer is logically owned for the duration of the task and
/// never aliased — the `Send` impl is safe in this confined usage. See
/// `try_run` for the lifecycle.
struct SendableRaw(*mut [u8]);
// SAFETY: see struct docs — exclusive ownership for the task's lifetime.
#[allow(unsafe_code)]
unsafe impl Send for SendableRaw {}

/// Result of one transfer:
/// - `Ok(conn)`        — run completed; the connection is alive.
/// - `Err(Some(conn))` — run failed but the connection survived.
/// - `Err(None)`       — run failed; the connection is gone.
///
/// Mirrors the carry-back-on-failure idiom used elsewhere in the codebase
/// (`Stream::close`, `Connection::request`).
type TransferResult = Result<Connection, Option<Connection>>;

async fn transfer(
    conn: Connection,
    track_id: [u8; TRACK_ID_LEN],
    buffer: WriteableBuffer,
    config: FecConfig,
    per_run_timeout: Duration,
    tx: &mpsc::UnboundedSender<DataEntry>,
) -> TransferResult {
    let run = async move {
        let pending = match conn.request(track_id.to_vec(), buffer, config).await {
            Ok(p) => p,
            Err((e, conn_back)) => {
                warn!("dc_receiver: request failed: {e:?}");
                return Err(Some(conn_back));
            }
        };

        let stream = match pending.ready().await {
            Ok(s) => s,
            Err((e, conn_back)) => {
                warn!("dc_receiver: stream-ready failed: {e:?}");
                return Err(Some(conn_back));
            }
        };

        // Periodic mid-stream drain. We can't poll `is_done` and `drain_data`
        // concurrently on the same `Stream` (drain borrows &self, complete
        // moves self), so we interleave: drain → check done → wait → drain.
        let mut drain_tick = tokio::time::interval(DRAIN_INTERVAL);
        drain_tick.tick().await; // skip the first immediate fire

        loop {
            tokio::select! {
                _ = drain_tick.tick() => {
                    for entry in stream.drain_data().await {
                        if tx.send(entry).is_err() {
                            // Caller dropped the receiver — abort gracefully.
                            let _ = stream.close().await;
                            return Err(None);
                        }
                    }
                }
                done = wait_done(&stream) => {
                    if done {
                        break;
                    }
                }
            }
        }

        match stream.complete().await {
            Ok((conn, trailing)) => {
                for entry in trailing {
                    if tx.send(entry).is_err() {
                        return Err(Some(conn));
                    }
                }
                Ok(conn)
            }
            Err(e) => {
                warn!("dc_receiver: complete() failed: {e:?}");
                Err(None)
            }
        }
    };

    if let Ok(outcome) = timeout(per_run_timeout, run).await {
        outcome
    } else {
        warn!("dc_receiver: transfer timed out after {per_run_timeout:?}");
        Err(None)
    }
}

/// Cheap "did the stream finish" check that doesn't move the Stream.
async fn wait_done<S: StreamTrait>(stream: &S) -> bool {
    // `is_done` polls a watch under the hood; budget a tiny await so the
    // select arm yields even if `is_done` is currently false.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.is_done().await
}

