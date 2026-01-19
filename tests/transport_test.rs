use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use ubass_rs::packet_processor::{PacketId, ProcessedPacket};
use ubass_rs::packetizer::PacketType;
use ubass_rs::packet_processor::TransportSendMessage;
use ubass_rs::transport::{
    SendablePacket, get_addr, get_session_token, init, recv, send, send_to_processing_layer,
};

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

// Integration test using init() API as intended.
// Simulates the processor layer by communicating through opaque channels.
// Uses a separate thread for the simulated client to avoid async runtime issues.
#[tokio::test]
async fn send_and_recv_full_integration() {
    use std::net::UdpSocket as StdUdpSocket;
    use std::time::Duration as StdDuration;
    use tokio::time::{Duration, timeout};

    let transport_port: u16 = 29880;
    let client_port: u16 = 29881;
    let session_token = get_session_token(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        client_port,
    ));

    // Create channels for communication with transport layer
    // to_transport: processor -> transport (send commands)
    // from_transport: transport -> processor (received packets)
    let (to_transport_tx, to_transport_rx) = tokio::sync::mpsc::channel(10);
    let (from_transport_tx, mut from_transport_rx) = tokio::sync::mpsc::channel(10);

    // Start transport layer via init()
    let init_handle = tokio::spawn(async move {
        init(transport_port, to_transport_rx, from_transport_tx).await
    });

    // Give transport time to bind sockets
    tokio::time::sleep(Duration::from_millis(100)).await;

    // === Test recv path ===
    // Spawn a client thread that sends UDP packets to the transport layer
    let test_message = b"hello_from_client";
    let client_handle = std::thread::spawn(move || {
        let client_socket = StdUdpSocket::bind(format!("127.0.0.1:{}", client_port))
            .expect("Failed to bind client socket");
        client_socket
            .set_read_timeout(Some(StdDuration::from_secs(5)))
            .unwrap();

        // Send packet to transport layer
        client_socket
            .send_to(test_message, format!("127.0.0.1:{}", transport_port))
            .expect("Failed to send to transport");

        // Wait for response from transport layer
        let mut buf = vec![0u8; 1024];
        match client_socket.recv_from(&mut buf) {
            Ok((len, _addr)) => Some(buf[..len].to_vec()),
            Err(_) => None,
        }
    });

    // Verify packet arrived on the from_transport channel
    let incoming = timeout(Duration::from_secs(2), from_transport_rx.recv()).await;
    match incoming {
        Ok(Some(Ok(packet))) => {
            assert_eq!(packet.data, test_message, "recv path: data mismatch");
        }
        Ok(Some(Err(e))) => panic!("recv path: received error: {:?}", e),
        Ok(None) => panic!("recv path: channel closed unexpectedly"),
        Err(_) => panic!("recv path: timeout waiting for packet"),
    }

    // === Test send path ===
    // Send a packet through the transport layer to the client
    let outgoing_packet = ProcessedPacket {
        packet_id: PacketId {
            timestamp: 200,
            session_token,
        },
        packet_type: PacketType::Data,
        data: b"hello_from_transport".to_vec(),
        duplicate_count: 1,
    };

    to_transport_tx
        .send(TransportSendMessage::Data(vec![outgoing_packet]))
        .await
        .expect("Failed to send to transport");

    // Verify client received the packet
    let client_received = client_handle.join().expect("Client thread panicked");
    assert_eq!(
        client_received,
        Some(b"hello_from_transport".to_vec()),
        "send path: client did not receive expected data"
    );

    // Clean shutdown via Close message
    to_transport_tx
        .send(TransportSendMessage::Close)
        .await
        .expect("Failed to send Close");

    // Wait for init to complete
    let _ = timeout(Duration::from_secs(1), init_handle).await;
}
