use std::time::Duration;

use tokio::time::timeout;
use tracing::{debug, info};
use ubass::{
    api::{Connection, ConnectionTrait, PendingStreamTrait, PlaybackControl, StreamTrait},
    prelude::packets::MAX_PAYLOAD_LENGTH,
};

pub fn playback_seek_client(message: Vec<u8>) -> impl AsyncFnOnce(Connection) {
    async move |connection: Connection| {
        debug!("trying to request stream with {}", connection.session_id());

        let buffer = vec![0u8; message.len()];
        let buffer = Box::into_raw(buffer.into());
        let mut id = message.clone();
        id.truncate(MAX_PAYLOAD_LENGTH);
        let pending = timeout(Duration::from_secs(2), connection.request(id, buffer))
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
        id.truncate(MAX_PAYLOAD_LENGTH);
        let pending = timeout(Duration::from_secs(2), connection.request(id, buffer))
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
        id.truncate(MAX_PAYLOAD_LENGTH);
        let pending = timeout(Duration::from_millis(900), connection.request(id, buffer))
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
