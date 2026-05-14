#![allow(clippy::unwrap_used)]
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use ubass::api::{Api, ConnectionTrait, PendingConnectionTrait, PendingStreamTrait, StreamTrait};
use ubass::prelude::packets::{FecConfig, FecScheme};

#[tokio::main]
pub async fn main() {
    let port = Some(ubass::DEFAULT_PORT);
    let app_id = "example client";
    let server_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12020);

    let api = Api::open(app_id, port).unwrap();

    let pending_connection = api.connect(server_address).await.unwrap();
    let connection = pending_connection.ready().await.unwrap();

    let track_id = b"some track ID";
    let buffer = vec![0u8; 42];

    let fec = FecConfig {
        scheme: FecScheme::Xor,
        recovery_count: 1,
        batch_size: 28,
    };
    let pending_stream = connection
        .request(track_id.as_slice(), buffer.as_slice(), fec)
        .await
        .unwrap();
    let stream = pending_stream.ready().await.unwrap();

    let (_connection, _entries) = stream.complete().await.unwrap();
}
