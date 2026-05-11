use std::{net::SocketAddr, time::Duration};

use tokio::time::timeout;
use tracing::debug;
use ubass::{
    Api,
    api::{IncomingConnectionTrait, PendingConnectionTrait},
};

pub async fn connection_refused_client(port: u16, app_id: String, server_addr: SocketAddr) {
    let api = Api::open(app_id, Some(port)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    debug!("trying to connect to {server_addr}");

    assert!(
        timeout(
            Duration::from_secs(2),
            timeout(Duration::from_secs(2), api.connect(server_addr))
                .await
                .unwrap()
                .unwrap()
                .ready(),
        )
        .await
        .unwrap()
        .is_err()
    );
}

pub async fn connection_refused_server(port: u16, app_id: String) {
    let api = Api::open(app_id, Some(port)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let incoming = match tokio::time::timeout(Duration::from_secs(10), api.listen())
        .await
        .unwrap()
        .unwrap()
    {
        ubass::api::AppEvent::IncomingConnection(incoming_connection) => incoming_connection,
        ubass::api::AppEvent::Closed => {
            panic!("closed");
        }
        ubass::api::AppEvent::ProtocolFailed(reason) => {
            panic!("protocol failed: {reason}");
        }
    };

    assert!(incoming.reject("420").await.is_ok());
}
