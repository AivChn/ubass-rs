//! Integration test scenarios for the `data-collection-api` wrappers.
//! Mirrors the `playback_control` module's shape: each scenario is a pair
//! of `*_server` / `*_client` async fns that drive the wrapper end-to-end.
//!
//! These functions are invoked by the integration binary's main dispatcher
//! based on the `--test` CLI flag; the e2e harness in `tests/e2e_test.rs`
//! spawns one process per side.

use std::net::SocketAddr;
use std::time::Duration;

use tracing::debug;

use ubass::api::{Receiver, Sender, SenderOptions};
use ubass::prelude::packets::{FecConfig, FecScheme};

/// How long the server keeps `Sender::serve` running before calling
/// `close()`. Has to outlast the client's sweep by a comfortable margin
/// (sweep ≈ 2 configs × 1 target × ~1 s each, conservative).
const SERVER_LIFETIME: Duration = Duration::from_secs(15);

/// Per-run timeout the client passes to `Receiver::sweep`. 5 s is plenty
/// for a 64 KiB transfer on loopback.
const PER_RUN_TIMEOUT: Duration = Duration::from_secs(5);

/// Buffer size for one transfer. Small enough to complete quickly under
/// loopback while still producing multiple data packets.
const BUFFER_LEN: usize = 64 * 1024;

/// Parallelism level for the receiver-side sliding window. The sweep has
/// one target, so this could be 1, but using 2 exercises the `JoinSet` path
/// even though only one slot is ever in flight.
const PARALLELISM: usize = 2;

/// Server: bring up `Sender::serve`, sink entries through a background
/// drain task, then `close()` after [`SERVER_LIFETIME`]. The client should
/// finish its sweep well before the close timer fires.
pub async fn data_collection_server(port: u16, app_id: String) {
    let mut sender =
        Sender::open(&app_id, Some(port), SenderOptions::default()).expect("Sender::open");
    let mut rx = sender.serve();

    // Background drain — the test only asserts the server doesn't crash
    // and that the client sees entries; the server's own rows just need to
    // not back up in the channel.
    let drain_task = tokio::spawn(async move {
        let mut count = 0u32;
        while rx.recv().await.is_some() {
            count += 1;
        }
        debug!("data_collection_server drained {count} entries");
    });

    tokio::time::sleep(SERVER_LIFETIME).await;
    sender.close().await;
    let _ = drain_task.await;
}

/// Client: open a `Receiver`, run a sweep over two FEC configs against
/// the single server target, collect entries, and assert the data path
/// produced rows with populated columns.
pub async fn data_collection_client(port: u16, app_id: String, server_addr: SocketAddr) {
    let receiver =
        Receiver::open(&app_id, Some(port), PARALLELISM).expect("Receiver::open");

    let configs = vec![
        FecConfig {
            scheme: FecScheme::Xor,
            recovery_count: 1,
            batch_size: 28,
        },
        FecConfig {
            scheme: FecScheme::ReedSolomon,
            recovery_count: 3,
            batch_size: 28,
        },
    ];

    let mut rx = receiver
        .sweep(vec![server_addr], configs, BUFFER_LEN, PER_RUN_TIMEOUT)
        .expect("sweep accepts valid buffer_len");

    let mut entries = Vec::new();
    while let Some(entry) = rx.recv().await {
        entries.push(entry);
    }

    debug!(
        "data_collection_client received {} entries; checking columns",
        entries.len()
    );

    assert!(!entries.is_empty(), "client should observe at least one DataEntry");

    let any_data_received = entries
        .iter()
        .any(|e| e.data_packets_received.is_some());
    assert!(
        any_data_received,
        "no entry has data_packets_received populated — the inbound packet-processor hook didn't fire"
    );

    let any_rtfb = entries
        .iter()
        .any(|e| e.request_to_first_byte_ms.is_some());
    assert!(
        any_rtfb,
        "no entry has request_to_first_byte_ms populated — the collector's RTFB synthesis didn't fire"
    );

    let any_fec = entries.iter().any(|e| {
        matches!(
            e.fec_config.scheme,
            FecScheme::Xor | FecScheme::ReedSolomon
        )
    });
    assert!(any_fec, "FecConfig didn't reach the entry rows");
}
