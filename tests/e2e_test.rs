#![allow(clippy::unwrap_used)]
#![warn(unused_must_use)]
use std::time::{Duration, Instant};

use derive_more::{Deref, DerefMut};

const TIMEOUT: Duration = Duration::from_secs(10);
const LONG_TIMEOUT: Duration = Duration::from_mins(2);

const BIN_PATH: &str = "/home/aiv/dev/ubass-rs/integration/target/debug/integration";

/// Wrap `std::process::Child` so any test that panics or returns early kills
/// (and reaps) the child instead of leaking it as an orphan with our stdout
/// fds still open.
#[derive(Deref, DerefMut)]
struct KillOnDrop(std::process::Child);

impl KillOnDrop {
    fn spawn(cmd: &mut std::process::Command, what: &str) -> Self {
        Self(
            cmd.spawn()
                .unwrap_or_else(|e| panic!("failed to spawn {what}: {e}")),
        )
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

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

fn prep_client_args<'a>(port: &'a str, test: &'a str, server_addr: &'a str) -> Vec<&'a str> {
    vec![
        "--side",
        "client",
        "--test",
        test,
        "--port",
        port,
        "--server-address",
        server_addr,
    ]
}

fn prep_server_args<'a>(test: &'a str, port: &'a str) -> Vec<&'a str> {
    vec!["--side", "server", "--test", test, "--port", port]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires single threaded"]
    fn connection_refused() {
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");
        let client_port = free_port();

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_server_args(
                "connection-refused",
                &server_port.to_string(),
            )),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(100));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "connection-refused",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }

    #[ignore = "requires single threaded"]
    #[test]
    fn e2e_echo() {
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");
        let client_port = free_port();

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH)
                .args(prep_server_args("echo", &server_port.to_string())),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(100));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "echo",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }

    #[ignore = "requires single threaded"]
    #[test]
    fn test_data_bigger_than_packet() {
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");
        let client_port = free_port();

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH)
                .args(prep_server_args("two-packets", &server_port.to_string())),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(200));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "two-packets",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }

    #[ignore = "requires single threaded"]
    #[test]
    fn very_big_data() {
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");
        let client_port = free_port();

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH)
                .args(prep_server_args("big-buffer", &server_port.to_string())),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(200));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "big-buffer",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
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

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH)
                .args(prep_server_args("audio-data", &server_port.to_string())),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(200));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "audio-data",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, LONG_TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, LONG_TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }

    #[test]
    #[ignore = "takes over 20 seconds"]
    fn audio_data_with_playback_control() {
        let client_port = free_port();
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_server_args(
                "audio-data-playback",
                &server_port.to_string(),
            )),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(200));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "audio-data-playback",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, LONG_TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, LONG_TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }

    #[ignore = "requires single threaded"]
    #[test]
    fn test_with_seek() {
        let client_port = free_port();
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH)
                .args(prep_server_args("seek", &server_port.to_string())),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(200));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "seek",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }

    #[ignore = "requires single threaded"]
    #[test]
    fn pause_play() {
        let client_port = free_port();
        let server_port = free_port();
        let server_address = format!("127.0.0.1:{server_port}");

        let mut server = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH)
                .args(prep_server_args("pause-play", &server_port.to_string())),
            "server",
        );

        // give the server time to bind and start listening
        std::thread::sleep(Duration::from_millis(200));

        let mut client = KillOnDrop::spawn(
            std::process::Command::new(BIN_PATH).args(prep_client_args(
                &client_port.to_string(),
                "pause-play",
                &server_address,
            )),
            "client",
        );

        let client_status = wait_timeout(&mut client, TIMEOUT).expect("client timed out");
        let server_status = wait_timeout(&mut server, TIMEOUT).expect("server timed out");

        assert!(
            client_status.success(),
            "client exited with: {client_status}"
        );
        assert!(
            server_status.success(),
            "server exited with: {server_status}"
        );
    }
}
