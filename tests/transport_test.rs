//! Comprehensive integration tests for the transport layer
//!
//! This test suite covers:
//! - Type conversions and basic structs
//! - Session token encoding/decoding (blackbox functions)
//! - Channel communication with processing layer
//! - UDP receive functionality
//! - UDP send functionality (single and multi-session)
//! - Duplicate packet transmission
//! - Full bidirectional integration via init()
//! - Error handling and edge cases
//! - Graceful shutdown

use core::panic;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use ubass_rs::packet_processor::{PacketId, ProcessedPacket, TransportSendMessage};
use ubass_rs::packetizer::PacketType;
use ubass_rs::transport::{
    SendablePacket, TransportError, get_addr, get_session_token, init, recv, send,
    send_to_processing_layer,
};

// ============================================================================
// Helper Functions
// ============================================================================

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
            session_token,
        },
        packet_type: PacketType::Data,
        data: data.to_vec(),
        duplicate_count,
    }
}

/// Creates a session token for a given port (for localhost testing)
fn token_for_port(port: u16) -> u128 {
    (port as u128) * 12 * 100_000_012
}

/// Finds an available port by binding to port 0
async fn find_available_port() -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.local_addr().unwrap().port()
}

// ============================================================================
// Type Conversion Tests
// ============================================================================

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

#[test]
fn sendable_packet_preserves_empty_data() {
    let processed = ProcessedPacket {
        packet_id: PacketId {
            timestamp: 0,
            session_token: 0,
        },
        packet_type: PacketType::Control,
        data: vec![],
        duplicate_count: 1,
    };

    let sendable: SendablePacket = SendablePacket::from(processed);

    assert!(sendable.data.is_empty());
    assert_eq!(sendable.duplicate_count, 1);
}

#[test]
fn sendable_packet_preserves_large_data() {
    let large_data: Vec<u8> = (0..1452).map(|i| (i % 256) as u8).collect();
    let processed = ProcessedPacket {
        packet_id: PacketId {
            timestamp: 999,
            session_token: 888,
        },
        packet_type: PacketType::Parity,
        data: large_data.clone(),
        duplicate_count: 5,
    };

    let sendable: SendablePacket = SendablePacket::from(processed);

    assert_eq!(sendable.data.len(), 1452);
    assert_eq!(sendable.data, large_data);
}

// ============================================================================
// Session Token Encoding/Decoding Tests (Blackbox Functions)
// ============================================================================

#[test]
fn get_addr_extracts_port_from_token() {
    // Token encodes port 6969
    let token = token_for_port(6969);
    let addr = get_addr(token);
    assert_eq!(addr, "127.0.0.1:6969");

    // Token encodes port 8080
    let token2 = token_for_port(8080);
    let addr2 = get_addr(token2);
    assert_eq!(addr2, "127.0.0.1:8080");
}

#[test]
fn get_session_token_encodes_port() {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 8080);
    let token = get_session_token(addr);
    assert_eq!(token, token_for_port(8080));
}

#[test]
fn session_token_roundtrip() {
    // Test that get_addr(get_session_token(addr)) recovers the port
    let port: u16 = 12345;
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), port);
    let token = get_session_token(addr);
    let recovered_addr = get_addr(token);
    assert_eq!(recovered_addr, format!("127.0.0.1:{}", port));
}

#[test]
fn different_ips_same_port_produce_same_token() {
    // Current implementation only encodes port, not IP
    let port: u16 = 54321;
    let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), port);
    let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), port);
    let addr3 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);

    let token1 = get_session_token(addr1);
    let token2 = get_session_token(addr2);
    let token3 = get_session_token(addr3);

    assert_eq!(token1, token2);
    assert_eq!(token2, token3);
}

#[test]
fn session_token_boundary_ports() {
    // Test edge case ports
    let port_min: u16 = 1;
    let port_max: u16 = 65535;

    let token_min = token_for_port(port_min);
    let token_max = token_for_port(port_max);

    assert_eq!(get_addr(token_min), "127.0.0.1:1");
    assert_eq!(get_addr(token_max), "127.0.0.1:65535");
}

// ============================================================================
// Channel Communication Tests
// ============================================================================

#[tokio::test]
async fn send_to_processing_layer_succeeds_with_capacity() {
    let (tx, mut rx) = mpsc::channel(10);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4023);

    let packet = SendablePacket {
        id: PacketId {
            timestamp: 1,
            session_token: get_session_token(addr),
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

#[tokio::test]
async fn send_to_processing_layer_fails_when_channel_closed() {
    let (tx, rx) = mpsc::channel::<Result<SendablePacket, TransportError>>(10);

    // Close the channel by dropping the receiver
    drop(rx);

    let packet = SendablePacket {
        id: PacketId {
            timestamp: 1,
            session_token: 0,
        },
        data: vec![1, 2, 3],
        duplicate_count: 0,
    };

    let result = send_to_processing_layer(tx, packet).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn send_to_processing_layer_multiple_packets() {
    let (tx, mut rx) = mpsc::channel(100);

    for i in 0..50 {
        let packet = SendablePacket {
            id: PacketId {
                timestamp: i as u128,
                session_token: 0,
            },
            data: vec![i as u8],
            duplicate_count: 0,
        };
        let result = send_to_processing_layer(tx.clone(), packet).await;
        assert!(result.is_ok());
    }

    // Verify all packets arrived in order
    for i in 0..50 {
        let received = rx.recv().await;
        if let Some(Ok(p)) = received {
            assert_eq!(p.id.timestamp, i as u128);
            assert_eq!(p.data, vec![i as u8]);
        } else {
            panic!("Expected packet {}", i);
        }
    }
}

// ============================================================================
// UDP Send Tests
// ============================================================================

#[tokio::test]
async fn send_empty_buffer_returns_ok() {
    let result = send(vec![]).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn send_transmits_single_packet() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    // Bind a listener
    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    let packets = vec![make_processed_packet(
        100,
        session_token,
        b"single_packet",
        1,
    )];

    let send_result = send(packets).await;
    assert!(send_result.is_ok(), "send() failed: {:?}", send_result);

    let mut buf = vec![0u8; 1024];
    let recv_result = timeout(Duration::from_secs(2), listener.recv_from(&mut buf)).await;

    match recv_result {
        Ok(Ok((len, _addr))) => {
            assert_eq!(&buf[..len], b"single_packet");
        }
        _ => panic!("Failed to receive packet"),
    }
}

#[tokio::test]
async fn send_transmits_multiple_packets_same_session() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    let packets = vec![
        make_processed_packet(100, session_token, b"packet_one", 1),
        make_processed_packet(101, session_token, b"packet_two", 1),
        make_processed_packet(102, session_token, b"packet_three", 1),
    ];

    let send_result = send(packets).await;
    assert!(send_result.is_ok());

    // Receive all three packets
    let mut received_data: Vec<Vec<u8>> = vec![];
    let mut buf = vec![0u8; 1024];

    for _ in 0..3 {
        let recv_result = timeout(Duration::from_secs(2), listener.recv_from(&mut buf)).await;
        match recv_result {
            Ok(Ok((len, _))) => {
                received_data.push(buf[..len].to_vec());
            }
            _ => panic!("Failed to receive packet"),
        }
    }

    // Verify all packets received (order may vary due to UDP)
    assert!(received_data.contains(&b"packet_one".to_vec()));
    assert!(received_data.contains(&b"packet_two".to_vec()));
    assert!(received_data.contains(&b"packet_three".to_vec()));
}

#[tokio::test]
async fn send_transmits_to_multiple_sessions() {
    let port1 = find_available_port().await;
    let port2 = find_available_port().await;
    let token1 = token_for_port(port1);
    let token2 = token_for_port(port2);

    let listener1 = UdpSocket::bind(format!("127.0.0.1:{}", port1))
        .await
        .unwrap();
    let listener2 = UdpSocket::bind(format!("127.0.0.1:{}", port2))
        .await
        .unwrap();

    let packets = vec![
        make_processed_packet(100, token1, b"for_session_1", 1),
        make_processed_packet(101, token2, b"for_session_2", 1),
    ];

    let send_result = send(packets).await;
    assert!(send_result.is_ok());

    // Verify each listener received the correct packet
    let mut buf1 = vec![0u8; 1024];
    let mut buf2 = vec![0u8; 1024];

    let recv1 = timeout(Duration::from_secs(2), listener1.recv_from(&mut buf1)).await;
    let recv2 = timeout(Duration::from_secs(2), listener2.recv_from(&mut buf2)).await;

    match recv1 {
        Ok(Ok((len, _))) => assert_eq!(&buf1[..len], b"for_session_1"),
        _ => panic!("Listener 1 failed to receive"),
    }

    match recv2 {
        Ok(Ok((len, _))) => assert_eq!(&buf2[..len], b"for_session_2"),
        _ => panic!("Listener 2 failed to receive"),
    }
}

#[tokio::test]
async fn send_respects_duplicate_count() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    // Send packet with duplicate_count = 3
    let packets = vec![make_processed_packet(
        100,
        session_token,
        b"duplicate_me",
        3,
    )];

    let send_result = send(packets).await;
    assert!(send_result.is_ok());

    // Should receive 3 copies
    let mut buf = vec![0u8; 1024];
    let mut count = 0;

    for _ in 0..3 {
        let recv_result = timeout(Duration::from_millis(500), listener.recv_from(&mut buf)).await;
        if let Ok(Ok((len, _))) = recv_result {
            assert_eq!(&buf[..len], b"duplicate_me");
            count += 1;
        }
    }

    assert_eq!(count, 3, "Expected 3 duplicate packets");

    // Fourth receive should timeout (no more packets)
    let extra = timeout(Duration::from_millis(100), listener.recv_from(&mut buf)).await;
    assert!(extra.is_err(), "Should not receive more than 3 packets");
}

#[tokio::test]
async fn send_with_zero_duplicate_count_sends_nothing() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    // Send packet with duplicate_count = 0
    let packets = vec![make_processed_packet(100, session_token, b"no_send", 0)];

    let send_result = send(packets).await;
    assert!(send_result.is_ok());

    // Should not receive anything
    let mut buf = vec![0u8; 1024];
    let recv_result = timeout(Duration::from_millis(200), listener.recv_from(&mut buf)).await;
    assert!(
        recv_result.is_err(),
        "Should not receive packet with duplicate_count=0"
    );
}

#[tokio::test]
async fn send_handles_large_packet() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    // Create max-size packet (1452 bytes)
    let large_data: Vec<u8> = (0..1452).map(|i| (i % 256) as u8).collect();
    let packets = vec![make_processed_packet(100, session_token, &large_data, 1)];

    let send_result = send(packets).await;
    assert!(send_result.is_ok());

    let mut buf = vec![0u8; 2000];
    let recv_result = timeout(Duration::from_secs(2), listener.recv_from(&mut buf)).await;

    match recv_result {
        Ok(Ok((len, _))) => {
            assert_eq!(len, 1452);
            assert_eq!(&buf[..len], large_data.as_slice());
        }
        _ => panic!("Failed to receive large packet"),
    }
}

// ============================================================================
// UDP Receive Tests
// ============================================================================

#[tokio::test]
async fn recv_receives_single_udp_packet() {
    let port = find_available_port().await;
    let (tx, mut rx) = mpsc::channel(10);

    // Spawn recv task
    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    // Wait for socket to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a UDP packet to the recv socket
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(b"test_message", format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Receive on channel
    let received = timeout(Duration::from_secs(2), rx.recv()).await;

    recv_handle.abort();

    match received {
        Ok(Some(Ok(packet))) => {
            assert_eq!(packet.data, b"test_message");
            assert_eq!(packet.duplicate_count, 0);
        }
        _ => panic!("Failed to receive packet"),
    }
}

#[tokio::test]
async fn recv_sets_session_token_from_source_address() {
    let port = find_available_port().await;
    let (tx, mut rx) = mpsc::channel(10);

    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Bind sender to specific port so we can predict the session token
    let sender_port = find_available_port().await;
    let sender = UdpSocket::bind(format!("127.0.0.1:{}", sender_port))
        .await
        .unwrap();
    sender
        .send_to(b"check_token", format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), rx.recv()).await;

    recv_handle.abort();

    match received {
        Ok(Some(Ok(packet))) => {
            // Session token should be derived from sender's address
            let expected_token = token_for_port(sender_port);
            assert_eq!(packet.id.session_token, expected_token);
        }
        _ => panic!("Failed to receive packet"),
    }
}

#[tokio::test]
async fn recv_handles_multiple_packets() {
    let port = find_available_port().await;
    let (tx, mut rx) = mpsc::channel(100);

    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    // Send multiple packets
    for i in 0..10 {
        let msg = format!("packet_{}", i);
        sender
            .send_to(msg.as_bytes(), format!("127.0.0.1:{}", port))
            .await
            .unwrap();
    }

    // Receive all packets
    let mut received_msgs: Vec<String> = vec![];
    for _ in 0..10 {
        let result = timeout(Duration::from_secs(2), rx.recv()).await;
        if let Ok(Some(Ok(packet))) = result {
            received_msgs.push(String::from_utf8(packet.data).unwrap());
        }
    }

    recv_handle.abort();

    // Verify all packets received (order may vary)
    assert_eq!(received_msgs.len(), 10);
    for i in 0..10 {
        assert!(received_msgs.contains(&format!("packet_{}", i)));
    }
}

#[tokio::test]
async fn recv_handles_packets_from_multiple_sources() {
    let port = find_available_port().await;
    let (tx, mut rx) = mpsc::channel(100);

    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Create multiple senders
    let sender1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let sender2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let sender1_port = sender1.local_addr().unwrap().port();
    let sender2_port = sender2.local_addr().unwrap().port();

    sender1
        .send_to(b"from_sender1", format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    sender2
        .send_to(b"from_sender2", format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Receive both packets
    let mut packets: Vec<SendablePacket> = vec![];
    for _ in 0..2 {
        if let Ok(Some(Ok(packet))) = timeout(Duration::from_secs(2), rx.recv()).await {
            packets.push(packet);
        }
    }

    recv_handle.abort();

    assert_eq!(packets.len(), 2);

    // Verify different session tokens for different sources
    let token1 = token_for_port(sender1_port);
    let token2 = token_for_port(sender2_port);

    let tokens: Vec<u128> = packets.iter().map(|p| p.id.session_token).collect();
    assert!(tokens.contains(&token1));
    assert!(tokens.contains(&token2));
}

#[tokio::test]
async fn recv_returns_error_when_channel_closes() {
    let port = find_available_port().await;
    let (tx, rx) = mpsc::channel::<Result<SendablePacket, TransportError>>(10);

    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Close the receiving end
    drop(rx);

    // Send a packet to trigger the send_to_processing_layer call
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(b"trigger", format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // recv() should return with an error
    let result = timeout(Duration::from_secs(2), recv_handle).await;

    match result {
        Ok(Ok(Err(TransportError::Internal(_)))) => {
            // Expected - internal error due to closed channel
        }
        other => panic!("Unexpected result: {:?}", other),
    }
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

    // Collect client ports and verify all messages received
    let client_ports: Vec<u16> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    let mut received_msgs: Vec<String> = vec![];
    let mut received_tokens: Vec<u128> = vec![];

    for _ in 0..num_clients {
        if let Ok(Some(Ok(packet))) =
            timeout(Duration::from_secs(2), from_transport_rx.recv()).await
        {
            received_msgs.push(String::from_utf8(packet.data).unwrap());
            received_tokens.push(packet.id.session_token);
        }
    }

    assert_eq!(received_msgs.len(), num_clients);

    // Verify each client's message was received
    for i in 0..num_clients {
        assert!(received_msgs.contains(&format!("client_{}", i)));
    }

    // Verify different session tokens for different clients
    let expected_tokens: Vec<u128> = client_ports.iter().map(|p| token_for_port(*p)).collect();
    for token in expected_tokens {
        assert!(received_tokens.contains(&token));
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
        session_token: 1,
    }]);
    let err2 = TransportError::CouldNotSend(vec![PacketId {
        timestamp: 2,
        session_token: 2,
    }]);

    // CouldNotSend variants are considered equal regardless of contents
    assert_eq!(err1, err2);

    let err3 = TransportError::FailedToBind;
    let err4 = TransportError::FailedToBind;

    assert_eq!(err3, err4);
    assert_ne!(err1, err3);
}

// ============================================================================
// Concurrent Operation Tests
// ============================================================================

#[tokio::test]
async fn concurrent_sends_to_same_session() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    let received_count = Arc::new(AtomicUsize::new(0));
    let received_count_clone = received_count.clone();

    // Spawn receiver task
    let recv_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        while received_count_clone.load(Ordering::SeqCst) < 10 {
            if let Ok(result) =
                timeout(Duration::from_millis(500), listener.recv_from(&mut buf)).await
            {
                if result.is_ok() {
                    received_count_clone.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    });

    // Send multiple batches concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let token = session_token;
        let handle = tokio::spawn(async move {
            let packets = vec![make_processed_packet(
                i as u128,
                token,
                format!("batch_{}", i).as_bytes(),
                1,
            )];
            send(packets).await
        });
        handles.push(handle);
    }

    // Wait for all sends to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    // Wait for receiver
    let _ = timeout(Duration::from_secs(5), recv_task).await;

    assert_eq!(received_count.load(Ordering::SeqCst), 10);
}

#[tokio::test]
async fn concurrent_sends_to_multiple_sessions() {
    let port1 = find_available_port().await;
    let port2 = find_available_port().await;
    let port3 = find_available_port().await;

    let listener1 = UdpSocket::bind(format!("127.0.0.1:{}", port1))
        .await
        .unwrap();
    let listener2 = UdpSocket::bind(format!("127.0.0.1:{}", port2))
        .await
        .unwrap();
    let listener3 = UdpSocket::bind(format!("127.0.0.1:{}", port3))
        .await
        .unwrap();

    // Send to all three sessions in one call
    let packets = vec![
        make_processed_packet(1, token_for_port(port1), b"to_1", 1),
        make_processed_packet(2, token_for_port(port2), b"to_2", 1),
        make_processed_packet(3, token_for_port(port3), b"to_3", 1),
    ];

    let result = send(packets).await;
    assert!(result.is_ok());

    // Verify each listener received
    let mut buf = vec![0u8; 1024];

    let r1 = timeout(Duration::from_secs(1), listener1.recv_from(&mut buf)).await;
    assert!(matches!(r1, Ok(Ok((4, _)))));
    assert_eq!(&buf[..4], b"to_1");

    let r2 = timeout(Duration::from_secs(1), listener2.recv_from(&mut buf)).await;
    assert!(matches!(r2, Ok(Ok((4, _)))));
    assert_eq!(&buf[..4], b"to_2");

    let r3 = timeout(Duration::from_secs(1), listener3.recv_from(&mut buf)).await;
    assert!(matches!(r3, Ok(Ok((4, _)))));
    assert_eq!(&buf[..4], b"to_3");
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
async fn send_with_mixed_duplicate_counts() {
    let dest_port = find_available_port().await;
    let session_token = token_for_port(dest_port);

    let listener = UdpSocket::bind(format!("127.0.0.1:{}", dest_port))
        .await
        .unwrap();

    let packets = vec![
        make_processed_packet(1, session_token, b"once", 1),
        make_processed_packet(2, session_token, b"twice", 2),
        make_processed_packet(3, session_token, b"thrice", 3),
    ];

    let result = send(packets).await;
    assert!(result.is_ok());

    // Should receive: 1 "once" + 2 "twice" + 3 "thrice" = 6 packets
    let mut received: Vec<Vec<u8>> = vec![];
    let mut buf = vec![0u8; 1024];

    for _ in 0..6 {
        if let Ok(Ok((len, _))) =
            timeout(Duration::from_millis(500), listener.recv_from(&mut buf)).await
        {
            received.push(buf[..len].to_vec());
        }
    }

    assert_eq!(received.len(), 6);
    assert_eq!(
        received.iter().filter(|d| d.as_slice() == b"once").count(),
        1
    );
    assert_eq!(
        received.iter().filter(|d| d.as_slice() == b"twice").count(),
        2
    );
    assert_eq!(
        received
            .iter()
            .filter(|d| d.as_slice() == b"thrice")
            .count(),
        3
    );
}

#[tokio::test]
async fn recv_handles_empty_packet() {
    let port = find_available_port().await;
    let (tx, mut rx) = mpsc::channel(10);

    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send empty packet
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(&[], format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), rx.recv()).await;

    recv_handle.abort();

    match received {
        Ok(Some(Ok(packet))) => {
            assert!(packet.data.is_empty());
        }
        _ => panic!("Failed to receive empty packet"),
    }
}

#[tokio::test]
async fn recv_handles_max_size_packet() {
    let port = find_available_port().await;
    let (tx, mut rx) = mpsc::channel(10);

    let recv_handle = tokio::spawn(async move { recv(port, tx).await });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send max-size packet
    let max_data: Vec<u8> = (0..1452).map(|i| (i % 256) as u8).collect();
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(&max_data, format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), rx.recv()).await;

    recv_handle.abort();

    match received {
        Ok(Some(Ok(packet))) => {
            assert_eq!(packet.data.len(), 1452);
            assert_eq!(packet.data, max_data);
        }
        _ => panic!("Failed to receive max-size packet"),
    }
}
