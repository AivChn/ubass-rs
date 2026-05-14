#![allow(clippy::wildcard_imports)]
use clap::Parser;
use std::fmt::Debug;
use std::fmt::Display;
use std::fs;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use tracing::debug;
use tracing::info;
use ubass::Api;
use ubass::api::Connection;
use ubass::api::ConnectionEvent;
use ubass::api::ConnectionTrait;
use ubass::api::IncomingConnectionTrait;
use ubass::api::PendingConnectionTrait;
use ubass::api::PendingStreamTrait;
use ubass::api::RequestedStreamTrait;
use ubass::api::StreamTrait;
use ubass::prelude::packets::MAX_PAYLOAD_LENGTH;
use ubass::prelude::packets::{FecConfig, FecScheme};

const FEC: FecConfig = FecConfig {
    scheme: FecScheme::Xor,
    recovery_count: 1,
    batch_size: 28,
};

mod connection_refused;
use connection_refused::*;

mod data_collection;
use data_collection::*;

mod playback_control;
use playback_control::*;

const LOREM_IPSUM: &str = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, \
    sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
    quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute \
    irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. \
    Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";

#[derive(Debug, clap::ValueEnum, Clone, Copy)]
enum Side {
    Server,
    Client,
}

impl Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Side::Server => "server",
                Side::Client => "client",
            }
        )
    }
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Test::Echo => "echo",
                Test::TwoPackets => "two_packets",
                Test::ConnectionRefused => "connection_refused",
                Test::BigBuffer => "big_buffer",
                Test::AudioData => "audio_data",
                Test::Seek => "seek_test",
                Test::PausePlay => "pause_play",
                Test::AudioDataPlayback => "audio_data_playback",
                Test::PauseAfterBufferDone => "pause_after_buffer_done",
                Test::TrackRequestRejected => "track_request_rejected",
                Test::MultiStream => "multi_stream",
                Test::DataCollection => "data_collection",
            }
        )
    }
}

#[derive(Clone, clap::ValueEnum)]
enum Test {
    Echo,
    TwoPackets,
    ConnectionRefused,
    BigBuffer,
    AudioData,
    Seek,
    PausePlay,
    AudioDataPlayback,
    PauseAfterBufferDone,
    TrackRequestRejected,
    MultiStream,
    DataCollection,
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    side: Side,
    #[arg(long)]
    test: Test,
    #[arg(long)]
    port: u16,
    #[arg(long)]
    server_address: Option<SocketAddr>,
}

#[tokio::main]
async fn main() {
    std::panic::set_hook(Box::new(|info| eprintln!("panicked in thread: {info}")));

    // get args
    let args = Args::parse();

    // set app id
    let app_id = format!("{}_{}", args.test, args.side);

    // set log handler
    let file = fs::File::create(format!("/home/aiv/dev/ubass-rs/tests/logs/{app_id}.log")).unwrap();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .pretty()
        .with_ansi(false)
        .with_writer(file)
        .init();

    // call function
    exec_test(args, app_id).await;
}

async fn exec_test(
    Args {
        side,
        test,
        port,
        server_address,
    }: Args,
    app_id: String,
) {
    match side {
        Side::Server => exec_server_test(test, port, app_id).await,
        Side::Client => {
            let server_addr = server_address.unwrap();
            exec_client_test(test, port, app_id, server_addr).await;
        }
    }
}

async fn exec_client_test(test: Test, port: u16, app_id: String, server_addr: SocketAddr) {
    match test {
        Test::Echo => {
            general_send_client(
                port,
                app_id,
                server_addr,
                LOREM_IPSUM.to_string().into_bytes(),
            )
            .await;
        }

        Test::TwoPackets => {
            general_send_client(
                port,
                app_id,
                server_addr,
                LOREM_IPSUM
                    .repeat((1500 / LOREM_IPSUM.len()) * 2)
                    .into_bytes(),
            )
            .await;
        }

        Test::ConnectionRefused => connection_refused_client(port, app_id, server_addr).await,

        Test::BigBuffer => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data.txt").unwrap();
            general_send_client(port, app_id, server_addr, data).await;
        }

        Test::AudioData => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/song.flac").unwrap();
            general_send_client(port, app_id, server_addr, data).await;
        }

        Test::Seek => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data2.txt").unwrap();
            client(port, app_id, server_addr, playback_seek_client(data)).await;
        }

        Test::PausePlay => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data2.txt").unwrap();
            client(port, app_id, server_addr, pause_play_test(data)).await;
        }

        Test::AudioDataPlayback => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/song.flac").unwrap();
            client(port, app_id, server_addr, audio_data_test(data)).await;
        }

        Test::PauseAfterBufferDone => {
            let data = LOREM_IPSUM.to_string().into_bytes();
            client(
                port,
                app_id,
                server_addr,
                pause_after_buffer_done_client(data),
            )
            .await;
        }

        Test::TrackRequestRejected => {
            client(port, app_id, server_addr, track_rejected_client()).await;
        }

        Test::MultiStream => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data2.txt").unwrap();
            client(
                port,
                app_id,
                server_addr,
                multi_stream_client(LOREM_IPSUM.to_string().into_bytes(), data.clone(), data),
            )
            .await;
        }

        Test::DataCollection => {
            data_collection_client(port, app_id, server_addr).await;
        }
    }
}

async fn exec_server_test(test: Test, port: u16, app_id: String) {
    match test {
        Test::Echo | Test::PauseAfterBufferDone => general_send_server(port, app_id, None).await,
        Test::TwoPackets => {
            general_send_server(
                port,
                app_id,
                Some(
                    LOREM_IPSUM
                        .repeat((1500 / LOREM_IPSUM.len()) * 2)
                        .into_bytes()
                        .into(),
                ),
            )
            .await;
        }

        Test::ConnectionRefused => connection_refused_server(port, app_id).await,
        Test::BigBuffer => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data.txt").unwrap();
            general_send_server(port, app_id, Some(data.into())).await;
        }
        Test::AudioData | Test::AudioDataPlayback => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/song.flac").unwrap();
            general_send_server(port, app_id, Some(data.into())).await;
        }
        Test::Seek | Test::PausePlay => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data2.txt").unwrap();
            general_send_server(port, app_id, Some(data.into())).await;
        }
        Test::TrackRequestRejected => server(port, app_id, track_rejected_server()).await,
        Test::MultiStream => {
            let data = fs::read("/home/aiv/dev/ubass-rs/tests/very_big_data2.txt").unwrap();
            server(
                port,
                app_id,
                multi_stream_server(LOREM_IPSUM.to_string().into_bytes(), data.clone(), data),
            )
            .await;
        }

        Test::DataCollection => {
            data_collection_server(port, app_id).await;
        }
    }
}

async fn general_send_server(port: u16, app_id: String, reply: Option<Box<[u8]>>) {
    let exec = async move |connection: Connection| {
        let ConnectionEvent::TrackRequested(requested) =
            tokio::time::timeout(Duration::from_secs(10), connection.listen())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("wrong event");
        };

        let buffer: Box<[u8]> = match reply {
            Some(reply) => reply,
            None => requested.track_id().to_vec().into_boxed_slice(),
        };

        let stream = requested
            .approve_and_ready(buffer)
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let (_connection, _entries) = stream.complete().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    server(port, app_id, exec).await;
}

async fn general_send_client(port: u16, app_id: String, server_addr: SocketAddr, message: Vec<u8>) {
    let exec = async move |connection: Connection| {
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

        debug!("waiting for stream to complete");
        let (_connection, _entries) = timeout(Duration::from_secs(50), stream.complete())
            .await
            .unwrap()
            .unwrap();

        let buffer = unsafe { Box::from_raw(buffer).to_vec() };
        assert_eq!(buffer, message.clone());
        let buffer_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        let message_rep = str::from_utf8(&buffer).unwrap_or("FAILED PARSING");
        info!("test passed! {buffer_rep} == {message_rep}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    client(port, app_id, server_addr, exec).await;
}

async fn client(
    port: u16,
    app_id: String,
    server_addr: SocketAddr,
    f: impl AsyncFnOnce(Connection),
) {
    let api = Api::open(app_id, Some(port)).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    debug!("trying to connect to {server_addr}");

    let connection = timeout(
        Duration::from_secs(2),
        timeout(Duration::from_secs(2), api.connect(server_addr))
            .await
            .unwrap()
            .unwrap()
            .ready(),
    )
    .await
    .unwrap()
    .unwrap();

    f(connection).await;
}

async fn server(port: u16, app_id: String, f: impl AsyncFnOnce(Connection)) {
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

    let connection = tokio::time::timeout(Duration::from_secs(10), incoming.approve_and_ready())
        .await
        .unwrap()
        .unwrap();

    f(connection).await;
}
