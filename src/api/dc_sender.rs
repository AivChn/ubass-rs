//! Server-side wrapper for offline data-collection sweeps. Companion to
//! [`Receiver`](super::Receiver) — accepts inbound connections, decodes the
//! `(seed, length)` track-id the receiver sent, generates the matching
//! [`SplitMix64`]-derived PRG buffer, and serves it back. Every
//! [`DataEntry`] produced by the collector during the transfer flows out
//! through the [`UnboundedReceiver`] returned by [`Sender::serve`].
//!
//! ### Resource posture
//! The server runs hotter than the client. Defaults reflect that:
//!
//! - `max_in_flight` (semaphore-bounded total concurrent streams) defaults
//!   to **64** vs. the receiver's small hardware-bound parallelism.
//! - `per_stream_timeout` defaults to **5 min** — the server is patient;
//!   a stuck transfer doesn't block others thanks to the in-flight cap.
//! - `max_buffer_len` defaults to **1 GiB** — four times the receiver's
//!   cap. The operator picks both `max_in_flight` and `max_buffer_len`,
//!   and the product is the worst-case resident memory floor.
//! - No peer authentication — incoming connections are auto-approved.
//!   The dataset is the only product; authenticating peers adds nothing.
//!
//! ### Lifecycle
//! Construct with [`Sender::open`], call [`Sender::serve`] once to start
//! the listen loop and obtain the data channel, drain entries from the
//! channel until you're done, then call [`Sender::close`] to tear down.
//! `close` consumes `self` and closes the underlying [`Api`] — to serve
//! again, open a new `Sender` (the protocol's global state requires a
//! fresh process anyway).
//!
//! [`UnboundedReceiver`]: tokio::sync::mpsc::UnboundedReceiver
//! [`DataEntry`]: crate::utils::DataEntry
//! [`SplitMix64`]: super::dc_proto::splitmix64_step

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::api::core::{Api, AppEvent, Connection, ConnectionEvent, IncomingConnection};
use crate::api::dc_proto::{decode_track_id, generate_prg_buffer};
use crate::api::types::{
    Connection as ConnectionTrait, IncomingConnection as IncomingConnectionTrait,
    RequestedStream as RequestedStreamTrait, Stream as StreamTrait,
};
use crate::error::ApiErrors;
use crate::utils::DataEntry;

/// How often a per-stream task forwards accumulated entries from the
/// collector. Matches the receiver's value — keeps the same end-to-end
/// latency budget on both sides.
const DRAIN_INTERVAL: Duration = Duration::from_millis(250);

/// Server-side tunables. All durations and sizes default to the values
/// the module-level docs describe; the operator overrides per-instance.
#[derive(Debug, Clone)]
pub struct SenderOptions {
    /// Hard cap on concurrent in-flight streams across all connections.
    /// Acquired via a [`Semaphore`] before generating the PRG buffer.
    pub max_in_flight: usize,
    /// Per-stream timeout. Once exceeded the stream is dropped on the
    /// floor; in-flight entries already forwarded are kept.
    pub per_stream_timeout: Duration,
    /// Cap on `length` decoded from a peer's track-id. A request beyond
    /// this is rejected with [`RequestedStream::reject`].
    ///
    /// [`RequestedStream::reject`]: crate::api::types::RequestedStream::reject
    pub max_buffer_len: usize,
    /// Shutdown grace: how long [`Sender::close`] waits for the listen
    /// loop to exit after the stop signal fires.
    pub shutdown_grace: Duration,
}

impl Default for SenderOptions {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            per_stream_timeout: Duration::from_mins(5),
            max_buffer_len: 1024 * 1024 * 1024, // 1 GiB
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

/// Server-side data-collection wrapper. See the [module docs](self).
pub struct Sender {
    api: Arc<Api>,
    options: SenderOptions,
    listen_task: Option<JoinHandle<()>>,
    stop_tx: Option<watch::Sender<bool>>,
}

impl Sender {
    /// Open the protocol with the given options.
    ///
    /// # Errors
    /// - Anything [`Api::open`] returns.
    /// - [`ApiErrors::InvalidPort`] re-used as a stand-in if
    ///   `options.max_in_flight` is `0` (no dedicated variant exists and a
    ///   zero-capacity semaphore would deadlock every transfer).
    pub fn open(
        app_id: &str,
        port: Option<u16>,
        options: SenderOptions,
    ) -> Result<Self, ApiErrors> {
        if options.max_in_flight == 0 {
            return Err(ApiErrors::InvalidPort);
        }
        Ok(Self {
            api: Arc::new(Api::open(app_id, port)?),
            options,
            listen_task: None,
            stop_tx: None,
        })
    }

    /// Start the listen loop. Returns the channel that yields every
    /// [`DataEntry`] produced by served transfers.
    ///
    /// # Panics
    /// Panics if called more than once on the same `Sender`. The contract
    /// is one listen loop per instance — to serve again, close this one
    /// and open a new `Sender`.
    pub fn serve(&mut self) -> mpsc::UnboundedReceiver<DataEntry> {
        assert!(
            self.listen_task.is_none(),
            "Sender::serve called twice on the same instance"
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let semaphore = Arc::new(Semaphore::new(self.options.max_in_flight));
        let api = self.api.clone();
        let options = self.options.clone();
        let handle = tokio::spawn(listen_loop(api, options, semaphore, stop_rx, tx));
        self.listen_task = Some(handle);
        self.stop_tx = Some(stop_tx);
        rx
    }

    /// Stop accepting new connections and tear down. Signals the listen
    /// loop to exit, awaits it up to [`SenderOptions::shutdown_grace`],
    /// then drops the protocol. In-flight per-stream tasks continue until
    /// their `per_stream_timeout` fires or the protocol's transport closes
    /// underneath them — they are not actively cancelled to avoid
    /// corrupting partial transfers the receiver is still verifying.
    pub async fn close(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.listen_task.take() {
            let _ = timeout(self.options.shutdown_grace, handle).await;
        }
        // self drops → Arc<Api> drops; if no other reference exists the
        // protocol shuts down via Api's existing cleanup path.
    }
}

async fn listen_loop(
    api: Arc<Api>,
    options: SenderOptions,
    semaphore: Arc<Semaphore>,
    mut stop_rx: watch::Receiver<bool>,
    tx: mpsc::UnboundedSender<DataEntry>,
) {
    loop {
        tokio::select! {
            biased;
            res = stop_rx.changed() => {
                if res.is_err() || *stop_rx.borrow() {
                    debug!("dc_sender: listen loop stopping");
                    break;
                }
            }
            event = api.listen() => {
                match event {
                    Ok(AppEvent::IncomingConnection(inc)) => {
                        tokio::spawn(serve_connection(
                            inc,
                            options.clone(),
                            semaphore.clone(),
                            stop_rx.clone(),
                            tx.clone(),
                        ));
                    }
                    Ok(AppEvent::Closed) => {
                        debug!("dc_sender: protocol closed");
                        break;
                    }
                    Ok(AppEvent::ProtocolFailed(reason)) => {
                        warn!("dc_sender: protocol failed: {reason:?}");
                        break;
                    }
                    Err(e) => {
                        warn!("dc_sender: listen error: {e:?}");
                        break;
                    }
                }
            }
        }
    }
}

async fn serve_connection(
    incoming: IncomingConnection,
    options: SenderOptions,
    semaphore: Arc<Semaphore>,
    mut stop_rx: watch::Receiver<bool>,
    tx: mpsc::UnboundedSender<DataEntry>,
) {
    let mut next_conn = match incoming.approve_and_ready().await {
        Ok(c) => Some(c),
        Err(e) => {
            warn!("dc_sender: approve_and_ready on incoming failed: {e:?}");
            return;
        }
    };

    while let Some(connection) = next_conn.take() {
        tokio::select! {
            biased;
            // Any wake on stop_rx is a shutdown signal — `stop_tx` only ever
            // sends `true` and channel-closed also means tear down. The
            // connection was moved into the listen-arm's future at select
            // construction; cancelling that future drops the connection,
            // which is the right behaviour on shutdown.
            _ = stop_rx.changed() => return,
            event = connection.listen() => {
                match event {
                    Ok(ConnectionEvent::TrackRequested(requested)) => {
                        // One-stream-at-a-time per connection is enforced
                        // by the core API (listen → requested → finalize →
                        // listen again on the returned connection).
                        let Ok(permit) = semaphore.clone().acquire_owned().await else {
                            return; // semaphore closed → bail
                        };
                        let outcome = serve_track(requested, &options, &tx).await;
                        drop(permit);
                        next_conn = outcome;
                    }
                    Ok(ConnectionEvent::ConnectionClosed | ConnectionEvent::ProtocolClosed) => {
                        debug!("dc_sender: connection closed by peer");
                        return;
                    }
                    Err(e) => {
                        warn!("dc_sender: connection.listen error: {e:?}");
                        return;
                    }
                }
            }
        }
    }
}

/// Serve one `TrackRequested` event. Returns the connection so the
/// per-connection loop can continue; `None` if the connection is gone.
async fn serve_track(
    requested: crate::api::core::RequestedStream<crate::api::types::Output>,
    options: &SenderOptions,
    tx: &mpsc::UnboundedSender<DataEntry>,
) -> Option<Connection> {
    let track_bytes = requested.track_id().to_vec();
    let Some((seed, length)) = decode_track_id(&track_bytes) else {
        warn!("dc_sender: malformed track_id ({} bytes)", track_bytes.len());
        return requested.reject().await.ok();
    };
    if length == 0 || (length as usize) > options.max_buffer_len {
        warn!(
            "dc_sender: rejecting length {length} (cap {})",
            options.max_buffer_len
        );
        return requested.reject().await.ok();
    }

    let buffer = generate_prg_buffer(seed, length as usize);
    let stream = match requested.approve_and_ready(buffer.into_vec()).await {
        Ok(s) => s,
        Err((e, conn)) => {
            warn!("dc_sender: approve_and_ready failed: {e:?}");
            return Some(conn);
        }
    };

    let run = async {
        let mut drain_tick = tokio::time::interval(DRAIN_INTERVAL);
        drain_tick.tick().await;

        loop {
            tokio::select! {
                _ = drain_tick.tick() => {
                    for entry in stream.drain_data().await {
                        if tx.send(entry).is_err() {
                            // Channel dropped — close cleanly and bail.
                            let _ = stream.close().await;
                            return None;
                        }
                    }
                }
                done = wait_done(&stream) => {
                    if done { break; }
                }
            }
        }

        match stream.complete().await {
            Ok((conn, trailing)) => {
                for entry in trailing {
                    if tx.send(entry).is_err() {
                        return Some(conn);
                    }
                }
                Some(conn)
            }
            Err(e) => {
                warn!("dc_sender: complete() failed: {e:?}");
                None
            }
        }
    };

    if let Ok(opt) = timeout(options.per_stream_timeout, run).await {
        opt
    } else {
        warn!(
            "dc_sender: transfer timed out after {:?}",
            options.per_stream_timeout
        );
        None
    }
}

async fn wait_done<S: StreamTrait>(stream: &S) -> bool {
    // Match the receiver-side polling cadence so both peers' rows
    // benefit from the same "drain at most every 250 ms" rhythm.
    tokio::time::sleep(Duration::from_millis(50)).await;
    stream.is_done().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_are_sensible() {
        let o = SenderOptions::default();
        assert!(o.max_in_flight > 0);
        assert!(o.per_stream_timeout > Duration::from_secs(1));
        assert!(o.max_buffer_len > 0);
        assert!(o.shutdown_grace > Duration::ZERO);
    }
}
