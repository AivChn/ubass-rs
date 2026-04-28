#![allow(clippy::unwrap_used)]
use std::net::SocketAddr;
use std::panic;
use std::time::Duration;
use tokio::time::timeout;
use ubass::api::ConnectionTrait;
use ubass::api::IncomingConnectionTrait;
use ubass::api::PendingConnectionTrait;
use ubass::api::StreamTrait;
use ubass::utils::ConnectionEvent;

use ubass::api::open;
use ubass::utils::PanicInDebug;

const MESSAGE: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, \
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
    quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute \
    irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. \
    Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ()> {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "server" => server(args[2].parse().unwrap()).await?,
        "client" => client(args[2].parse().unwrap(), args[3].parse().unwrap()).await?,
        _ => panic!("usage: e2e_peer <server|client> <port> [server_addr]"),
    }
    Ok(())
}

async fn server(port: u16) -> Result<(), ()> {
    let api = open("e2e-server".to_string(), Some(port))
        .await
        .map_err(|_| println!("open"))?;

    let incoming = match tokio::time::timeout(Duration::from_secs(10), api.listen())
        .await
        .map_err(|_| println!("listen timout"))?
        .map_err(|_| println!("listen"))?
    {
        ubass::api::AppEvent::IncomingConnection(incoming_connection) => incoming_connection,
        ubass::api::AppEvent::DataReceived { .. } => {
            eprintln!("data");
            return Err(());
        }
        ubass::api::AppEvent::Closed => {
            eprintln!("closed");
            return Err(());
        }
    };

    let mut connection =
        tokio::time::timeout(Duration::from_secs(10), incoming.approve_and_ready())
            .await
            .map_err(|_| println!("incoming timeout"))?
            .map_err(|_| println!("incoming"))?;

    let id = match tokio::time::timeout(Duration::from_secs(10), connection.listen())
        .await
        .map_err(|_| println!("listen timeout"))?
    {
        Ok(ConnectionEvent::TrackRequest(id)) => id,
        _ => {
            return Err(());
        }
    };

    let stream = connection.send(id).await.unwrap();
    _ = stream.complete().await.panic_in_debug("This happened");
    Ok(())
}

async fn client(port: u16, server_addr: SocketAddr) -> Result<(), ()> {
    let api = open("e2e-client".to_string(), Some(port))
        .await
        .map_err(|_| println!("api open"))?;

    let connection = timeout(
        Duration::from_secs(2),
        timeout(Duration::from_secs(2), api.connect(server_addr))
            .await
            .map_err(|_| println!("connect timeout"))?
            .map_err(|_| println!("connect"))?
            .ready(),
    )
    .await
    .map_err(|_| println!("ready timeout"))?
    .map_err(|_| println!("ready failed!"))?;

    let mut buffer = vec![0; MESSAGE.len()];
    let stream = timeout(
        Duration::from_secs(2),
        connection.request(MESSAGE, buffer.as_mut_slice()),
    )
    .await
    .map_err(|_| println!("request timeout"))?
    .map_err(|_| println!("request"))?;

    let _connection = timeout(Duration::from_secs(10), stream.complete())
        .await
        .map_err(|_| println!("stream complete timeout"))?
        .map_err(|_| println!("stream complete error"))?;

    println!("{}", str::from_utf8(&buffer).unwrap());
    assert_eq!(buffer, MESSAGE);
    Ok(())
}
