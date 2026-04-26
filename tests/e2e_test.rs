use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(10);
//comment

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

#[test]
fn e2e_echo() {
    let server_port = free_port();
    let client_port = free_port();
    let binary = "/home/aiv/dev/ubass-rs/target/debug/e2e_peer";

    let mut server = std::process::Command::new(binary)
        .args(["server", &server_port.to_string()])
        .spawn()
        .expect("failed to spawn server");

    // give the server time to bind and start listening
    std::thread::sleep(Duration::from_millis(200));

    let mut client = std::process::Command::new(binary)
        .args([
            "client",
            &client_port.to_string(),
            &format!("127.0.0.1:{server_port}"),
        ])
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
