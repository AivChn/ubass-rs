#![allow(clippy::unwrap_used)]
use ubass::{
    Api,
    api::{
        AppEvent, ConnectionEvent, ConnectionTrait, IncomingConnectionTrait, RequestedStreamTrait,
        StreamTrait,
    },
};

#[tokio::main]
pub async fn main() {
    // Open the API
    let api = Api::open("minimal server example", Some(999)).unwrap();

    // Wait for an incoming request for connection
    let AppEvent::IncomingConnection(incoming) = api.listen().await.unwrap() else {
        return;
    };

    // Approve connection
    let connection = incoming.approve().await.unwrap();

    // Get the requested stream, nothing else
    let ConnectionEvent::TrackRequested(requested_stream) = connection.listen().await.unwrap()
    else {
        return;
    };

    // Stream the data
    let buffer = Vec::from(b"So long, and thanks for all the fish!");
    let stream = requested_stream.approve(buffer).await.unwrap();

    // Close stream then connection
    let connection = stream.complete().await.unwrap();
    connection.close().await;
}
