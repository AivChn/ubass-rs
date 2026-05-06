use clap::{Parser, Subcommand};
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use tracing::debug;
use tracing::info;
use ubass::api::ConnectionTrait;
use ubass::api::IncomingConnectionTrait;
use ubass::api::PendingConnectionTrait;
use ubass::api::StreamTrait;
use ubass::api::open;
use ubass::prelude::packets::MAX_PAYLOAD_LENGTH;
use ubass::utils::ConnectionEvent;

#[derive(Subcommand, Clone)]
enum Side {
    Server {
        #[arg(long)]
        echo: bool,
    },
    Client {
        #[arg(short, long)]
        server_address: SocketAddr,
    },
}

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    port: u16,
    #[arg(short, long)]
    name: String,
    #[arg(long)]
    message: String,
    #[arg(long)]
    path: bool,
    #[command(subcommand)]
    side: Side,
}

#[tokio::main]
async fn main() -> Result<(), ()> {
    std::panic::set_hook(Box::new(|info| eprintln!("panicked in thread: {info}")));
    let args = Args::parse();
    match args.side {
        Side::Server { echo } => {
            let file = fs::File::create(format!(
                "/home/aiv/dev/ubass-rs/tests/logs/server_{}.log",
                args.name
            ))
            .unwrap();
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(file)
                .pretty()
                .with_ansi(false)
                .init();

            let reply = (!echo).then_some(if args.path {
                fs::read(args.message).unwrap()
            } else {
                args.message.into_bytes()
            });
            server(args.port, reply.map(Box::from)).await
        }
        Side::Client { server_address } => {
            let file = fs::File::create(format!(
                "/home/aiv/dev/ubass-rs/tests/logs/client_{}.log",
                args.name
            ))
            .unwrap();
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(file)
                .pretty()
                .with_ansi(false)
                .init();
            let message = if args.path {
                fs::read(args.message).unwrap()
            } else {
                args.message.into_bytes()
            };
            client(args.port, server_address, message).await
        }
    }
}

async fn server(port: u16, reply: Option<Box<[u8]>>) -> Result<(), ()> {
    let api = open("e2e-server".to_string(), Some(port))
        .await
        .map_err(|_| println!("open"))?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let incoming = match tokio::time::timeout(Duration::from_secs(10), api.listen())
        .await
        .map_err(|_| println!("listen timout"))?
        .map_err(|_| println!("listen"))?
    {
        ubass::api::AppEvent::IncomingConnection(incoming_connection) => incoming_connection,
        ubass::api::AppEvent::Closed => {
            eprintln!("closed");
            return Err(());
        }
    };

    let mut connection =
        tokio::time::timeout(Duration::from_secs(10), incoming.approve_and_ready())
            .await
            .map_err(|_| println!("incoming timeout"))?
            .map_err(|e| println!("incoming: {e}"))?;

    let Ok(ConnectionEvent::TrackRequest(id)) =
        tokio::time::timeout(Duration::from_secs(10), connection.listen())
            .await
            .map_err(|_| println!("listen timeout"))?
    else {
        return Err(());
    };

    let stream = match reply {
        Some(reply) => connection.send(reply).await.unwrap(),
        None => connection.send(id).await.unwrap(),
    };

    _ = stream.complete().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

async fn client(port: u16, server_addr: SocketAddr, message: Vec<u8>) -> Result<(), ()> {
    let api = open("e2e-client".to_string(), Some(port))
        .await
        .map_err(|_| println!("api open"))?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    debug!("trying to connect to {server_addr}");

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

    debug!("opened connection with {}", connection.session_id());
    debug!("trying to request stream with {}", connection.session_id());

    let buffer = vec![0u8; message.len()];
    let buffer = Box::into_raw(buffer.into());
    let mut id = message.clone();
    id.truncate(MAX_PAYLOAD_LENGTH);
    let stream = timeout(Duration::from_secs(2), connection.request(id, buffer))
        .await
        .map_err(|_| println!("request timeout"))?
        .map_err(|_| println!("request"))?;

    debug!("waiting for stream to complete");
    let _connection = timeout(Duration::from_secs(50), stream.complete())
        .await
        .map_err(|_| "stream complete timeout")
        .unwrap()
        .unwrap();

    let buffer = unsafe { Box::from_raw(buffer).to_vec() };
    assert_eq!(buffer, message.clone());
    let buffer_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
    let message_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
    info!("test passed! {buffer_rep} == {message_rep}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}
