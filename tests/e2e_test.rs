#![allow(clippy::unwrap_used)]
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    thread,
    time::{Duration, Instant},
};

use ubass::api::{AppEvent, IncomingConnectionTrait, PendingConnectionTrait};

const TIMEOUT: Duration = Duration::from_secs(10);
//comment

const LOREM_IPSUM: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, \
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
    quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute \
    irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. \
    Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";

const BIN_PATH: &str = "/home/aiv/dev/ubass-rs/integration/target/debug/integration";

/// Bind to port 0 to get a free UDP port from the OS.
fn free_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> Option<std::process::ExitStatus> {
    let start = Instant::now();
    loop {
        match child.try_wait().unwrap() {
            Some(status) => return Some(status),
            None if start.elapsed() >= timeout => {
                child.kill().ok();
                return None;
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

fn prep_client_args<'a>(
    port: &'a str,
    name: &'a str,
    message: &'a str,
    is_path: bool,
    address: &'a str,
) -> Vec<&'a str> {
    let mut v = vec!["--port", port, "--name", name, "--message", message];

    if is_path {
        v.push("--path");
    }
    v.extend(&["client", "--server-address", address]);

    v
}

fn prep_server_args<'a>(
    port: &'a str,
    name: &'a str,
    message: &'a str,
    is_path: bool,
    is_echo: bool,
) -> Vec<&'a str> {
    let mut v = vec!["--port", port, "--name", name, "--message", message];
    if is_path {
        v.push("--path");
    }
    v.push("server");
    if is_echo {
        v.push("--echo");
    }
    v
}

#[test]
fn connection_refused() {
    let server_port = free_port();
    let client_port = free_port();

    let server = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async move {
            let api = ubass::open("server_connection_refused", Some(server_port))
                .await
                .unwrap();

            eprintln!("listening on {server_port}");
            let AppEvent::IncomingConnection(incoming) = api.listen().await.unwrap() else {
                panic!("expected IncomingConnection");
            };

            assert!(incoming.reject("420").await.is_ok());
        });
    });

    std::thread::sleep(Duration::from_millis(200));

    let client = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async move {
            let api = ubass::open("client_connection_refused", Some(client_port))
                .await
                .unwrap();

            let pending = api
                .connect(SocketAddrV4::new(Ipv4Addr::LOCALHOST, server_port).into())
                .await
                .unwrap();

            assert!(pending.ready().await.is_err());
        })
    });
    server.join().unwrap();
    client.join().unwrap();
}

#[test]
fn e2e_echo() {
    let server_port = free_port();
    let server_address = format!("127.0.0.1:{server_port}");
    let client_port = free_port();

    let mut server = std::process::Command::new(BIN_PATH)
        .args(prep_server_args(
            &server_port.to_string(),
            "echo",
            "",
            false,
            true,
        ))
        .spawn()
        .expect("failed to spawn server");

    // give the server time to bind and start listening
    std::thread::sleep(Duration::from_millis(200));

    let mut client = std::process::Command::new(BIN_PATH)
        .args(prep_client_args(
            &client_port.to_string(),
            "echo",
            LOREM_IPSUM,
            false,
            &server_address,
        ))
        .spawn()
        .expect("failed to spawn client");

    let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
    assert!(
        client_status.success(),
        "client exited with: {client_status}"
    );

    let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");
    assert!(
        server_status.success(),
        "server exited with: {server_status}"
    );
}

#[test]
fn test_data_bigger_than_packet() {
    let server_port = free_port();
    let server_address = format!("127.0.0.1:{server_port}");
    let client_port = free_port();
    let message = LOREM_IPSUM.repeat((1500 / LOREM_IPSUM.len()) * 2);

    let mut server = std::process::Command::new(BIN_PATH)
        .args(prep_server_args(
            &server_port.to_string(),
            "bigger",
            &message,
            false,
            false,
        ))
        .spawn()
        .expect("failed to spawn server");

    // give the server time to bind and start listening
    std::thread::sleep(Duration::from_millis(200));

    let mut client = std::process::Command::new(BIN_PATH)
        .args(prep_client_args(
            &client_port.to_string(),
            "bigger",
            &message,
            false,
            &server_address,
        ))
        .spawn()
        .expect("failed to spawn client");

    let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
    assert!(
        client_status.success(),
        "client exited with: {client_status}"
    );

    let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");
    assert!(
        server_status.success(),
        "server exited with: {server_status}"
    );
}

#[test]
fn very_big_data() {
    let server_port = free_port();
    let server_address = format!("127.0.0.1:{server_port}");
    let client_port = free_port();
    let message_path = "/home/aiv/dev/ubass-rs/tests/very_big_data.txt";

    let mut server = std::process::Command::new(BIN_PATH)
        .args(prep_server_args(
            &server_port.to_string(),
            "very big",
            message_path,
            true,
            false,
        ))
        .spawn()
        .expect("failed to spawn server");

    // give the server time to bind and start listening
    std::thread::sleep(Duration::from_millis(200));

    let mut client = std::process::Command::new(BIN_PATH)
        .args(prep_client_args(
            &client_port.to_string(),
            "very big",
            message_path,
            true,
            &server_address,
        ))
        .spawn()
        .expect("failed to spawn client");

    let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
    assert!(
        client_status.success(),
        "client exited with: {client_status}"
    );

    let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");
    assert!(
        server_status.success(),
        "server exited with: {server_status}"
    );
}

#[test]
#[ignore = "takes 19 seconds to run"]
fn audio_data() {
    let server_port = free_port();
    let server_address = format!("127.0.0.1:{server_port}");
    let client_port = free_port();
    let message_path = "/home/aiv/dev/ubass-rs/tests/song.flac";

    let mut server = std::process::Command::new(BIN_PATH)
        .args(prep_server_args(
            &server_port.to_string(),
            "song",
            message_path,
            true,
            false,
        ))
        .spawn()
        .expect("failed to spawn server");

    // give the server time to bind and start listening
    std::thread::sleep(Duration::from_millis(200));

    let mut client = std::process::Command::new(BIN_PATH)
        .args(prep_client_args(
            &client_port.to_string(),
            "song",
            message_path,
            true,
            &server_address,
        ))
        .spawn()
        .expect("failed to spawn client");

    let client_status =
        wait_timeout(&mut client, Duration::from_secs(120)).expect("client timed out");
    assert!(
        client_status.success(),
        "client exited with: {client_status}"
    );

    let server_status =
        wait_timeout(&mut server, Duration::from_secs(120)).expect("server timed out");
    assert!(
        server_status.success(),
        "server exited with: {server_status}"
    );
}
