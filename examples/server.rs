#![allow(clippy::unwrap_used)]
use ubass::{
    Api,
    api::{
        AppEvent, ApprovalStatus, ConnectionTrait, IncomingConnectionTrait, RequestedStreamTrait,
        StreamTrait,
    },
};

#[tokio::main]
pub async fn main() {
    let port = Some(ubass::DEFAULT_PORT);
    let app_id = "example server";

    let api = Api::open(app_id, port).await.unwrap();

    let event = api.listen().await.unwrap();
    let incoming_connection = match event {
        AppEvent::IncomingConnection(incoming_connection) => incoming_connection,
        AppEvent::ProtocolFailed(error) => {
            println!("Failed! {error}");
            return;
        }
        AppEvent::Closed => return,
    };

    let connection = incoming_connection
        .approve_if_and_ready(|app_id| app_id == "example client", "Not example Client")
        .await
        .unwrap() // was not example client
        .unwrap(); // connection failed for some reason

    let event = connection.listen().await.unwrap();
    let requested_stream = match event {
        ubass::api::ConnectionEvent::TrackRequested(requested_stream) => requested_stream,
        ubass::api::ConnectionEvent::ConnectionClosed => return,
        ubass::api::ConnectionEvent::ProtocolClosed => return,
    };

    let mut data = b"So long, and thanks for all the fish!".to_vec();
    data.resize(42, 0);
    let approval_status = requested_stream
        .approve_if_and_ready(|track_id| track_id == b"some track ID".as_slice(), data)
        .await;

    let stream = match approval_status {
        ApprovalStatus::Approved(result) => match result {
            Ok(stream) => stream,
            Err((error, connection)) => {
                println!("failed to connect!");
                return;
            }
        },
        ApprovalStatus::Rejected(result) => {
            match result {
                Ok(connection) => println!("Rejected successfuly"),
                Err(error) => println!("error!"),
            }
            return;
        }
    };

    let _connection = stream.complete().await.unwrap();
}
