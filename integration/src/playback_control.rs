use std::time::Duration;

use tokio::time::timeout;
use tracing::{debug, info};
use ubass::{
    api::{
        Connection, ConnectionEvent, ConnectionTrait, PendingStreamTrait, PlaybackControl,
        RequestedStreamTrait, StreamTrait,
    },
    error::ConnectionError,
    prelude::packets::{FecConfig, FecScheme, MAX_PAYLOAD_LENGTH},
};

const FEC: FecConfig = FecConfig {
    scheme: FecScheme::Xor,
    recovery_count: 1,
    batch_size: 28,
};

pub fn multi_stream_server(
    msg1: Vec<u8>,
    msg2: Vec<u8>,
    msg3: Vec<u8>,
) -> impl AsyncFnOnce(Connection) {
    async move |connection| {
        let ConnectionEvent::TrackRequested(req) = connection.listen().await.unwrap() else {
            panic!("not track request");
        };
        let stream = req.approve_and_ready(msg1).await.unwrap();

        let (connection, _entries) = stream.complete().await.unwrap();

        let ConnectionEvent::TrackRequested(req) = connection.listen().await.unwrap() else {
            panic!("not track request");
        };
        let stream = req.approve_and_ready(msg2).await.unwrap();

        let (connection, _entries) = stream.complete().await.unwrap();

        let ConnectionEvent::TrackRequested(req) = connection.listen().await.unwrap() else {
            panic!("not track request");
        };
        let stream = req.approve_and_ready(msg3).await.unwrap();

        let (connection, _entries) = stream.complete().await.unwrap();
        _ = connection;
    }
}

pub fn multi_stream_client(
    msg1: Vec<u8>,
    msg2: Vec<u8>,
    msg3: Vec<u8>,
) -> impl AsyncFnOnce(Connection) {
    async move |connection| {
        debug!(
            "trying to request stream 1 with {}",
            connection.session_id()
        );

        let mut buffer = vec![0u8; msg1.len()];
        let stream = connection
            .request(b"track 1".to_vec(), buffer.as_slice(), FEC)
            .await
            .unwrap()
            .ready()
            .await
            .unwrap();

        let (connection, _entries) = stream.complete().await.unwrap();

        assert_eq!(buffer, msg1);

        debug!(
            "trying to request stream 2 with {}",
            connection.session_id()
        );

        buffer.resize(msg2.len(), 0);
        buffer.fill(0);

        let stream = connection
            .request(b"track 2".to_vec(), buffer.as_slice(), FEC)
            .await
            .unwrap()
            .ready()
            .await
            .unwrap();

        let connection = stream.close().await.unwrap();

        assert_ne!(buffer, msg2);

        debug!(
            "trying to request stream 3 with {}",
            connection.session_id()
        );

        buffer.resize(msg3.len(), 0);
        buffer.fill(0);

        let stream = connection
            .request(b"track 3".to_vec(), buffer.as_slice(), FEC)
            .await
            .unwrap()
            .ready()
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;
        stream.pause().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        stream.play().await.unwrap();

        let (connection, _entries) = stream.complete().await.unwrap();
        _ = connection;
    }
}

pub fn track_rejected_client() -> impl AsyncFnOnce(Connection) {
    async move |connection| {
        debug!("trying to request stream with {}", connection.session_id());

        let buffer = vec![0u8; 1];
        let buffer = Box::into_raw(buffer.into());
        let id = b"The answer to everything".to_vec();
        let pending = timeout(Duration::from_secs(2), connection.request(id, buffer, FEC))
            .await
            .unwrap()
            .unwrap();
        assert!(
            pending
                .ready()
                .await
                .is_err_and(|e| matches!(e.0, ConnectionError::PeerRejected(_)))
        );
    }
}

pub fn track_rejected_server() -> impl AsyncFnOnce(Connection) {
    async move |connection| {
        let event = connection.listen().await.unwrap();
        let ConnectionEvent::TrackRequested(request) = event else {
            panic!("wrong event!");
        };

        assert!(request.reject().await.is_ok());
    }
}

pub fn pause_after_buffer_done_client(message: Vec<u8>) -> impl AsyncFnOnce(Connection) {
    async move |connection: Connection| {
        debug!("trying to request stream with {}", connection.session_id());

        let buffer = vec![0u8; message.len()];
        let buffer = Box::into_raw(buffer.into());
        let mut id = message.clone();
        id.truncate(MAX_PAYLOAD_LENGTH - 3);
        let pending = timeout(Duration::from_secs(2), connection.request(id, buffer, FEC))
            .await
            .unwrap()
            .unwrap();
        let stream = pending.ready().await.map_err(|(e, _)| e).unwrap();

        while !stream.is_done().await {}

        assert!(stream.pause().await.is_ok());
        assert!(stream.play().await.is_ok());

        debug!("waiting for stream to complete");
        _ = timeout(Duration::from_secs(10), stream.complete())
            .await
            .unwrap()
            .unwrap();

        let buffer = unsafe { Box::from_raw(buffer).to_vec() };
        assert_eq!(buffer, message.clone());
        let buffer_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        let message_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        info!("test passed! {buffer_rep} == {message_rep}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn playback_seek_client(message: Vec<u8>) -> impl AsyncFnOnce(Connection) {
    async move |connection: Connection| {
        debug!("trying to request stream with {}", connection.session_id());

        let buffer = vec![0u8; message.len()];
        let buffer = Box::into_raw(buffer.into());
        let mut id = message.clone();
        id.truncate(MAX_PAYLOAD_LENGTH - 3);
        let pending = timeout(Duration::from_secs(2), connection.request(id, buffer, FEC))
            .await
            .unwrap()
            .unwrap();
        let stream = pending.ready().await.map_err(|(e, _)| e).unwrap();

        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(stream.seek(message.len() / 2).await.is_ok());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(stream.seek(0).await.is_ok());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(stream.seek(message.len() * 2).await.is_ok());

        debug!("waiting for stream to complete");
        _ = timeout(Duration::from_secs(10), stream.complete())
            .await
            .unwrap()
            .unwrap();

        let buffer = unsafe { Box::from_raw(buffer).to_vec() };
        assert_eq!(buffer, message.clone());
        let buffer_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        let message_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        info!("test passed! {buffer_rep} == {message_rep}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn pause_play_test(message: Vec<u8>) -> impl AsyncFnOnce(Connection) {
    async move |connection: Connection| {
        debug!("trying to request stream with {}", connection.session_id());

        let buffer = vec![0u8; message.len()];
        let buffer = Box::into_raw(buffer.into());
        let mut id = message.clone();
        id.truncate(MAX_PAYLOAD_LENGTH - 3);
        let pending = timeout(Duration::from_secs(2), connection.request(id, buffer, FEC))
            .await
            .unwrap()
            .unwrap();
        let stream = pending.ready().await.map_err(|(e, _)| e).unwrap();

        assert!(stream.pause().await.is_ok());
        assert!(stream.pause().await.is_ok());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(stream.play().await.is_ok());

        debug!("waiting for stream to complete");
        _ = timeout(Duration::from_secs(50), stream.complete())
            .await
            .unwrap()
            .unwrap();

        let buffer = unsafe { Box::from_raw(buffer).to_vec() };
        assert_eq!(buffer, message.clone());
        let buffer_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        let message_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        info!("test passed! {buffer_rep} == {message_rep}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn audio_data_test(message: Vec<u8>) -> impl AsyncFnOnce(Connection) {
    async move |connection: Connection| {
        debug!("trying to request stream with {}", connection.session_id());

        let buffer = vec![0u8; message.len()];
        let buffer = Box::into_raw(buffer.into());
        let mut id = message.clone();
        id.truncate(MAX_PAYLOAD_LENGTH - 3);
        let pending = timeout(Duration::from_millis(900), connection.request(id, buffer, FEC))
            .await
            .unwrap()
            .unwrap();
        let stream = pending.ready().await.map_err(|(e, _)| e).unwrap();

        let first_seek = buffer.len() / 2;
        let second_seek = first_seek / 2;
        let third_seek = second_seek * 3 - 300;

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(stream.seek(first_seek).await.is_ok());
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(stream.seek(second_seek).await.is_ok());

        assert!(stream.pause().await.is_ok());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(stream.play().await.is_ok());

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(stream.seek(third_seek).await.is_ok());

        debug!("waiting for stream to complete");
        _ = timeout(Duration::from_mins(2), stream.complete())
            .await
            .unwrap()
            .unwrap();

        let buffer = unsafe { Box::from_raw(buffer).to_vec() };
        assert_eq!(buffer, message.clone());
        let buffer_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        let message_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        info!("test passed! {buffer_rep} == {message_rep}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
