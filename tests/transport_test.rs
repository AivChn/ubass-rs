//! Comprehensive integration tests for the transport layer
//!
//! This test suite covers:
//! - Full bidirectional integration via init()
//! - Multiple concurrent clients
//! - Graceful shutdown

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use crate::packet_processor::{PacketId, ProcessedPacket, TransportSendMessage};
use crate::packetizer::PacketType;
use crate::transport::{TransportError, get_session_token, init};

// ============================================================================
// Helper Functions
// ============================================================================

// TODO: fix broken test
/// Creates a ProcessedPacket with the given parameters
fn make_processed_packet(
    timestamp: u128,
    session_token: u128,
    data: &[u8],
    duplicate_count: usize,
) -> ProcessedPacket {
    ProcessedPacket {
        packet_id: PacketId {
            timestamp,
            session_id: session_token,
        },
        packet_type: PacketType::Data,
        data: data.to_vec(),
        duplicate_count,
    }
}

/// Creates a session token for a given port (for localhost testing)
fn token_for_port(port: u16) -> u128 {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    get_session_token(addr)
}

/// Finds an available port by binding to port 0
async fn find_available_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.local_addr().unwrap().port()
}

// ============================================================================
// Full Integration Tests with init()
// ============================================================================

#[tokio::test]
async fn init_recv_path_integration() {
    let transport_port = find_available_port().await;

    let (to_transport_tx, to_transport_rx) = mpsc::channel(10);
    let (from_transport_tx, mut from_transport_rx) = mpsc::channel(10);

    // Start transport layer
    let init_handle =
        tokio::spawn(async move { init(transport_port, to_transport_rx, from_transport_tx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send UDP packet to transport
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client
        .send_to(b"hello_transport", format!("127.0.0.1:{}", transport_port))
        .await
        .unwrap();

    // Verify packet received on channel
    let result = timeout(Duration::from_secs(2), from_transport_rx.recv()).await;

    match result {
        Ok(Some(Ok(packet))) => {
            assert_eq!(packet.data, b"hello_transport");
        }
        _ => panic!("Failed to receive packet through init()"),
    }

    // Shutdown
    to_transport_tx
        .send(TransportSendMessage::Close)
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(1), init_handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn init_send_path_integration() {
    use std::net::UdpSocket as StdUdpSocket;
    use std::time::Duration as StdDuration;

    let transport_port = find_available_port().await;
    let client_port = find_available_port().await;
    let session_token = token_for_port(client_port);

    let (to_transport_tx, to_transport_rx) = mpsc::channel(10);
    let (from_transport_tx, _from_transport_rx) = mpsc::channel(10);

    let init_handle =
        tokio::spawn(async move { init(transport_port, to_transport_rx, from_transport_tx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start client in separate thread
    let client_handle = std::thread::spawn(move || {
        let client = StdUdpSocket::bind(format!("127.0.0.1:{}", client_port)).unwrap();
        client
            .set_read_timeout(Some(StdDuration::from_secs(5)))
            .unwrap();

        let mut buf = vec![0u8; 1024];
        match client.recv_from(&mut buf) {
            Ok((len, _)) => Some(buf[..len].to_vec()),
            Err(_) => None,
        }
    });

    // Send packet through transport layer
    let packet = make_processed_packet(100, session_token, b"hello_client", 1);
    to_transport_tx
        .send(TransportSendMessage::Data(vec![packet]))
        .await
        .unwrap();

    // Verify client received
    let client_data = client_handle.join().unwrap();
    assert_eq!(client_data, Some(b"hello_client".to_vec()));

    // Shutdown
    to_transport_tx
        .send(TransportSendMessage::Close)
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(1), init_handle).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn init_bidirectional_integration() {
    use std::net::UdpSocket as StdUdpSocket;
    use std::time::Duration as StdDuration;

    let transport_port = find_available_port().await;
    let client_port = find_available_port().await;
    let session_token = token_for_port(client_port);

    let (to_transport_tx, to_transport_rx) = mpsc::channel(10);
    let (from_transport_tx, mut from_transport_rx) = mpsc::channel(10);

    let init_handle = tokio::spawn(init(transport_port, to_transport_rx, from_transport_tx));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client thread: send to transport, then wait for response
    let transport_port_clone = transport_port;
    let client_handle = std::thread::spawn(move || {
        let client = StdUdpSocket::bind(format!("127.0.0.1:{}", client_port)).unwrap();
        client
            .set_read_timeout(Some(StdDuration::from_secs(5)))
            .unwrap();

        // Send to transport
        client
            .send_to(
                b"request_data",
                format!("127.0.0.1:{}", transport_port_clone),
            )
            .unwrap();

        // Wait for response
        let mut buf = vec![0u8; 1024];
        let (len, _) = client.recv_from(&mut buf).expect("Didnt get any data");
        Some(buf[..len].to_vec())
    });

    // Verify transport received client's message
    let incoming = timeout(Duration::from_secs(2), from_transport_rx.recv()).await;
    match incoming {
        Ok(Some(Ok(packet))) => {
            assert_eq!(packet.data, b"request_data");
        }
        _ => panic!("Transport didn't receive client message"),
    }

    // Send response back to client
    let response = make_processed_packet(200, session_token, b"response_data", 1);
    to_transport_tx
        .send(TransportSendMessage::Data(vec![response]))
        .await
        .unwrap();

    // Verify client got response
    let client_data = client_handle.join().unwrap();
    assert_eq!(client_data, Some(b"response_data".to_vec()));

    // Shutdown
    to_transport_tx
        .send(TransportSendMessage::Close)
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(1), init_handle).await;
}

#[tokio::test]
async fn init_handles_multiple_clients() {
    use std::net::UdpSocket as StdUdpSocket;
    use std::time::Duration as StdDuration;

    let transport_port = find_available_port().await;

    let (to_transport_tx, to_transport_rx) = mpsc::channel(100);
    let (from_transport_tx, mut from_transport_rx) = mpsc::channel(100);

    let init_handle =
        tokio::spawn(async move { init(transport_port, to_transport_rx, from_transport_tx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn multiple clients
    let num_clients = 3;
    let mut client_handles = vec![];

    for i in 0..num_clients {
        let port = transport_port;
        let handle = std::thread::spawn(move || {
            let client = StdUdpSocket::bind("127.0.0.1:0").unwrap();
            client
                .set_read_timeout(Some(StdDuration::from_secs(5)))
                .unwrap();

            let msg = format!("client_{}", i);
            client
                .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
                .unwrap();

            client.local_addr().unwrap().port()
        });
        client_handles.push(handle);
    }

    // Collect client ports
    let _client_ports: Vec<u16> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let mut received_msgs: Vec<String> = vec![];

    for _ in 0..num_clients {
        if let Ok(Some(Ok(packet))) =
            timeout(Duration::from_secs(2), from_transport_rx.recv()).await
        {
            received_msgs.push(String::from_utf8(packet.data).unwrap());
        }
    }

    assert_eq!(received_msgs.len(), num_clients);

    // Verify each client's message was received
    for i in 0..num_clients {
        assert!(received_msgs.contains(&format!("client_{}", i)));
    }

    // Shutdown
    to_transport_tx
        .send(TransportSendMessage::Close)
        .await
        .unwrap();

    let _ = timeout(Duration::from_secs(1), init_handle).await;
}

#[tokio::test]
async fn init_graceful_shutdown() {
    let transport_port = find_available_port().await;

    let (to_transport_tx, to_transport_rx) = mpsc::channel(10);
    let (from_transport_tx, _from_transport_rx) = mpsc::channel(10);

    let init_handle =
        tokio::spawn(async move { init(transport_port, to_transport_rx, from_transport_tx).await });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send close message
    to_transport_tx
        .send(TransportSendMessage::Close)
        .await
        .unwrap();

    // init() should return Ok(())
    let result = timeout(Duration::from_secs(2), init_handle).await;

    match result {
        Ok(Ok(Ok(()))) => {
            // Expected - graceful shutdown
        }
        other => panic!("Expected graceful shutdown, got: {:?}", other),
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn transport_error_partial_eq() {
    let err1 = TransportError::CouldNotSend(vec![PacketId {
        timestamp: 1,
        session_id: 1,
    }]);
    let err2 = TransportError::CouldNotSend(vec![PacketId {
        timestamp: 2,
        session_id: 2,
    }]);

    // CouldNotSend variants are considered equal regardless of contents
    assert_eq!(err1, err2);

    let err3 = TransportError::FailedToBind;
    let err4 = TransportError::FailedToBind;

    assert_eq!(err3, err4);
    assert_ne!(err1, err3);
}
