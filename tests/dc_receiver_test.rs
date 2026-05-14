//! Pure-Rust tests for the `data-collection-api` receiver wrapper.
//!
//! The happy-path test (sweep → real peer → verify PRG) needs the future
//! server-side wrapper to act as the responder. Until that lands, this
//! suite covers the wrapper's pre-conditions and error paths only:
//!
//! - `buffer_len` validation
//! - Connect-failure handling (target on a closed port)
//!
//! Everything runs in a single `#[tokio::test]` because the protocol's
//! global `STATE` `OnceLock` permits exactly one `Receiver::open` per
//! process lifetime.

#![cfg(feature = "data-collection-api")]
// Tests have looser conventions than library code; the project denies these
// at the lib level so opt back in here.
#![allow(clippy::unwrap_used, clippy::err_expect)]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use ubass::api::{MAX_BUFFER_LEN, Receiver};
use ubass::error::ApiErrors;
use ubass::prelude::packets::{FecConfig, FecScheme};

fn fec() -> FecConfig {
    FecConfig {
        scheme: FecScheme::Xor,
        recovery_count: 1,
        batch_size: 28,
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn wrapper_validation_and_connect_failure_paths() {
    let port = free_port();
    let recv = Receiver::open("dc_receiver_test", Some(port), 4).unwrap();

    // 1. Zero-length buffer must be rejected before anything else happens.
    let target = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), free_port());
    let err = recv
        .sweep(vec![target], vec![fec()], 0, Duration::from_secs(1))
        .err()
        .expect("sweep with zero buffer_len must fail");
    assert!(matches!(err, ApiErrors::BufferTooLarge));

    let err = recv
        .run_once(target, fec(), 0, Duration::from_secs(1))
        .await
        .err()
        .expect("run_once with zero buffer_len must fail");
    assert!(matches!(err, ApiErrors::BufferTooLarge));

    // 2. Oversize buffer (above MAX_BUFFER_LEN) must be rejected too.
    let err = recv
        .sweep(
            vec![target],
            vec![fec()],
            MAX_BUFFER_LEN + 1,
            Duration::from_secs(1),
        )
        .err()
        .expect("oversize sweep must fail");
    assert!(matches!(err, ApiErrors::BufferTooLarge));

    // 3. Connect-failure path: target on port 1 has no listener, so the
    //    handshake will time out. The wrapper should log + skip and
    //    return an empty Vec without panicking.
    let unreachable = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
    let entries = recv
        .run_once(unreachable, fec(), 4096, Duration::from_millis(300))
        .await
        .expect("validation passes; connect-fail path returns Ok(empty)");
    assert!(
        entries.is_empty(),
        "no entries should be produced when handshake never completes"
    );
}
