#![allow(clippy::unwrap_used)]
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use ubass::api::{Api, ConnectionTrait, PendingConnectionTrait, PendingStreamTrait, StreamTrait};

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

    let pending_stream = connection
        .request(track_id.as_slice(), buffer.as_slice())
        .await
        .unwrap();
    let stream = pending_stream.ready().await.unwrap();

    let _connection = stream.complete().await.unwrap();
}
