use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use ubass_rs::packet_processor::{PacketId, ProcessedPacket};
use ubass_rs::packetizer::PacketType;
use ubass_rs::transport::{
    SendablePacket, TransportError, get_addr, get_session_token, recv, send,
    send_to_processing_layer,
};

/// There are a few useless tests to be removed.

#[test]
fn sendable_packet_from_processed_packet() {
    let processed = ProcessedPacket {
        packet_id: PacketId {
            timestamp: 42,
            session_token: 123456,
        },
        packet_type: PacketType::Data,
        data: vec![1, 2, 3, 4, 5],
        duplicate_count: 3,
    };

    let sendable: SendablePacket = SendablePacket::from(processed.clone());

    assert_eq!(sendable.id.timestamp, 42);
    assert_eq!(sendable.id.session_token, 123456);
    assert_eq!(sendable.data, vec![1, 2, 3, 4, 5]);
    assert_eq!(sendable.duplicate_count, 3);
}

/// Remove - no shit? its a static definition, this is basically just saying what the compiler
/// already confirmed.
#[test]
fn transport_error_equality_could_not_send() {
    let err1 = TransportError::CouldNotSend(vec![PacketId {
        timestamp: 1,
        session_token: 100,
    }]);
    let err2 = TransportError::CouldNotSend(vec![PacketId {
        timestamp: 2,
        session_token: 200,
    }]);

    // CouldNotSend variants are equal regardless of contents
    assert_eq!(err1, err2);
}

/// do i even need to explain? yes, 1+1 does indeed equal to 2.
#[test]
fn transport_error_equality_faild_to_bind() {
    let err1 = TransportError::FaildToBind;
    let err2 = TransportError::FaildToBind;

    assert_eq!(err1, err2);
}

/// same here. the definition is static in code and is not affected by the runtime.
#[test]
fn transport_error_inequality_different_variants() {
    let err1 = TransportError::FaildToBind;
    let err2 = TransportError::CouldNotSend(vec![]);

    assert_ne!(err1, err2);
}

/// testing blackbox functions. these will be replaced anyways
#[test]
fn get_addr_extracts_port_from_token() {
    // Token encodes port 6969: 6969 * 12 * 100_000_012
    let token = 6969 * 12 * 100_000_012;
    let addr = get_addr(token);
    assert_eq!(addr, "127.0.0.1:6969");

    // Token encodes port 8080
    let token2 = 8080 * 12 * 100_000_012;
    let addr2 = get_addr(token2);
    assert_eq!(addr2, "127.0.0.1:8080");
}

/// testing blackbox functions. these will be replaced anyways
#[test]
fn get_session_token_encodes_port() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
    let token = get_session_token(addr);
    // Token should be port * 12 * 100_000_012
    assert_eq!(token, 8080 * 12 * 100_000_012);
}

#[test]
fn session_token_roundtrip() {
    // Test that get_addr(get_session_token(addr)) recovers the port
    let port: u16 = 12345;
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), port);
    let token = get_session_token(addr);
    let recovered_addr = get_addr(token);
    assert_eq!(recovered_addr, format!("127.0.0.1:{}", port));

    // Different IPs with same port should produce same token (only port matters)
    let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port);
    let token2 = get_session_token(addr2);
    assert_eq!(token, token2);
}

#[tokio::test]
async fn send_empty_buffer_returns_ok() {
    let result = send(vec![]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_to_processing_layer_succeeds_with_capacity() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4023);

    let packet = SendablePacket {
        id: PacketId {
            timestamp: 1,
            session_token: get_session_token(addr2),
        },
        data: vec![1, 2, 3],
        duplicate_count: 0,
    };

    let result = send_to_processing_layer(tx, packet.clone()).await;
    assert!(result.is_ok());

    let received = rx.recv().await;
    assert!(received.is_some());
    if let Some(Ok(p)) = received {
        assert_eq!(p.id.timestamp, 1);
        assert_eq!(p.data, vec![1, 2, 3]);
    }
}

// Integration test using netcat to send UDP data to the recv function
#[tokio::test]
async fn recv_receives_udp_packet_from_netcat() {
    use std::process::Command;
    use tokio::time::{Duration, timeout};

    let port: u16 = 29876; // Use a high port to avoid conflicts
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    // Spawn the recv task
    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    // Give the socket time to bind
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Use netcat to send a UDP packet - MUST use std::thread, not tokio::spawn
    // because tokio's single-threaded test runtime has scheduling issues with
    // Command::new().output() inside tokio::spawn while awaiting socket.recv_from()
    let test_message = "hello_from_netcat";
    let port_clone = port;
    let msg_clone = test_message.to_string();
    std::thread::spawn(move || {
        let _ = Command::new("netcat")
            .args(["127.0.0.1", &port_clone.to_string(), &msg_clone, "udp"])
            .output();
    });

    // Wait for the packet to be received (with timeout)
    let received = timeout(Duration::from_secs(2), rx.recv()).await;

    // Abort the recv task since it loops forever
    recv_handle.abort();

    match received {
        Ok(Some(Ok(packet))) => {
            assert_eq!(packet.data, test_message.as_bytes());
        }
        Ok(Some(Err(e))) => {
            panic!("Received error: {:?}", e);
        }
        Ok(None) => {
            panic!("Channel closed unexpectedly");
        }
        Err(_) => {
            panic!("Timeout waiting for packet");
        }
    }
}

// Integration test: send() transmits packets over UDP to the destination
// Session token encodes the destination port via get_addr()
#[tokio::test]
async fn send_transmits_packets_over_udp() {
    use tokio::net::UdpSocket;
    use tokio::time::{Duration, timeout};

    let dest_port: u16 = 6969;
    let session_token = (dest_port as u128) * 12 * 100_000_012;

    // Bind a listener on the port encoded in session_token
    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    // Create test packets with session_token that encodes dest_port
    let packets = vec![
        ProcessedPacket {
            packet_id: PacketId {
                timestamp: 100,
                session_token,
            },
            packet_type: PacketType::Data,
            data: b"packet_one".to_vec(),
            duplicate_count: 1,
        },
        ProcessedPacket {
            packet_id: PacketId {
                timestamp: 101,
                session_token,
            },
            packet_type: PacketType::Data,
            data: b"packet_two".to_vec(),
            duplicate_count: 1,
        },
    ];

    // Call send() - this should transmit packets to 127.0.0.1:dest_port
    let send_result = send(packets).await;
    assert!(send_result.is_ok(), "send() failed: {:?}", send_result);

    // Receive the packets on the listener
    let mut buf = vec![0u8; 1024];

    // First packet
    let recv1 = timeout(Duration::from_secs(2), listener.recv_from(&mut buf)).await;
    match recv1 {
        Ok(Ok((len, _addr))) => {
            assert_eq!(&buf[..len], b"packet_one");
        }
        _ => panic!("Failed to receive first packet"),
    }

    // Second packet
    let recv2 = timeout(Duration::from_secs(2), listener.recv_from(&mut buf)).await;
    match recv2 {
        Ok(Ok((len, _addr))) => {
            assert_eq!(&buf[..len], b"packet_two");
        }
        _ => panic!("Failed to receive second packet"),
    }
}

// Integration test: both send and recv work together in the same test
// - recv listens on one port, forwards incoming packets to channel
// - send transmits packets out to a different port (to avoid conflict with other tests)
// - verifies both directions work independently
//
// == Editors Note ==
// I have a few issues with this test. First, it doesnt utilize the API that is given to it. the
// original code had only one public function - init. init receives all the information needed to
// get the transport going, and provides the send/recv functions with their channels. An end to end
// test of the transport should simulate the processor layer by calling the init function and
// communicating through the opaque channels with either.
// In addition, this test is doomed to fail since there is a 2 second timout set for *receiving a
// UDP packet*. it will surely get dropped by then, since UDP does not care about reliability. My
// suggestion would be to run everything through init() as intended and run a client on a seperate
// thread to simulate real world conditions.
#[tokio::test]
async fn send_and_recv_full_integration() {
    use std::process::Command;
    use tokio::net::UdpSocket;
    use tokio::time::{Duration, timeout};

    let recv_port: u16 = 29879;
    let send_dest_port: u16 = 6970; // Different from send_transmits_packets_over_udp test
    let session_token = (send_dest_port as u128) * 12 * 100_000_012;

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    // Start recv task
    let recv_handle = tokio::spawn(async move { recv(recv_port, tx).await });

    // Start listener for outgoing packets
    let outgoing_listener = UdpSocket::bind(format!("127.0.0.1:{}", send_dest_port))
        .await
        .unwrap();

    // Give sockets time to bind
    tokio::time::sleep(Duration::from_millis(150)).await;

    // === Test recv path: send packet via netcat, verify it arrives on channel ===
    let recv_test_msg = "incoming_packet";
    let port_clone = recv_port;
    let msg_clone = recv_test_msg.to_string();
    std::thread::spawn(move || {
        let _ = Command::new("netcat")
            .args(["127.0.0.1", &port_clone.to_string(), &msg_clone, "udp"])
            .output();
    });

    let incoming = timeout(Duration::from_secs(2), rx.recv()).await;
    match incoming {
        Ok(Some(Ok(packet))) => {
            assert_eq!(
                packet.data,
                recv_test_msg.as_bytes(),
                "recv path: data mismatch"
            );
        }
        _ => panic!("recv path: failed to receive incoming packet"),
    }

    // === Test send path: call send(), verify packet arrives at listener ===
    let outgoing_packets = vec![ProcessedPacket {
        packet_id: PacketId {
            timestamp: 200,
            session_token,
        },
        packet_type: PacketType::Data,
        data: b"outgoing_packet".to_vec(),
        duplicate_count: 0,
    }];

    let send_result = send(outgoing_packets).await;
    assert!(send_result.is_ok(), "send path: send() failed");

    let mut buf = vec![0u8; 1024];
    let outgoing = timeout(
        Duration::from_secs(2),
        outgoing_listener.recv_from(&mut buf),
    )
    .await;
    match outgoing {
        Ok(Ok((len, _addr))) => {
            assert_eq!(&buf[..len], b"outgoing_packet", "send path: data mismatch");
        }
        _ => panic!("send path: failed to receive outgoing packet"),
    }

    // Cleanup
    recv_handle.abort();
}
