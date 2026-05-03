use clap::{Parser, Subcommand};
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
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
        #[arg(short, long)]
        reply: Option<String>,
    },
    Client {
        #[arg(short, long)]
        server_address: SocketAddr,
        #[arg(short, long)]
        message: String,
    },
}

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    port: u16,
    #[arg(short, long)]
    name: String,
    #[command(subcommand)]
    side: Side,
}

#[tokio::main]
async fn main() -> Result<(), ()> {
    std::panic::set_hook(Box::new(|info| eprintln!("panicked in thread: {info}")));
    let args = Args::parse();
    match args.side {
        Side::Server { reply } => {
            let file = fs::File::create(format!(
                "/home/aiv/dev/ubass-rs/logs/server_{}.log",
                args.name
            ))
            .unwrap();
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(file)
                .pretty()
                .with_ansi(false)
                .init();
            server(args.port, reply).await
        }
        Side::Client {
            server_address,
            message,
        } => {
            let file = fs::File::create(format!(
                "/home/aiv/dev/ubass-rs/logs/client_{}.log",
                args.name
            ))
            .unwrap();
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(file)
                .pretty()
                .with_ansi(false)
                .init();

            client(args.port, server_address, message).await
        }
    }
}

async fn server(port: u16, reply: Option<String>) -> Result<(), ()> {
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
            .map_err(|_| println!("incoming"))?;

    let Ok(ConnectionEvent::TrackRequest(id)) =
        tokio::time::timeout(Duration::from_secs(10), connection.listen())
            .await
            .map_err(|_| println!("listen timeout"))?
    else {
        return Err(());
    };

    let stream = match reply {
        Some(reply) => connection.send(reply.into_bytes()).await.unwrap(),
        None => connection.send(id).await.unwrap(),
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    _ = stream.complete().await.unwrap();
    Ok(())
}

async fn client(port: u16, server_addr: SocketAddr, message: String) -> Result<(), ()> {
    let api = open("e2e-client".to_string(), Some(port))
        .await
        .map_err(|_| println!("api open"))?;

    tokio::time::sleep(Duration::from_millis(50)).await;

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

    let mut buffer = vec![0; message.len()];
    let mut id = message.clone().into_bytes();
    id.truncate(MAX_PAYLOAD_LENGTH);
    let stream = timeout(
        Duration::from_secs(2),
        connection.request(id, buffer.as_mut_slice()),
    )
    .await
    .map_err(|_| println!("request timeout"))?
    .map_err(|_| println!("request"))?;

    let _connection = timeout(Duration::from_secs(10), stream.complete())
        .await
        .map_err(|_| println!("stream complete timeout"))?
        .map_err(|_| println!("stream complete error"))?;

    assert_eq!(buffer, message.clone().into_bytes());
    //print!("{} == {}", str::from_utf8(&buffer).unwrap(), &message);
    Ok(())
}
