#![allow(warnings)]

use crate::{
    InternalError,
    packet_processor::{PacketId, ProcessedPacket, TransportSendMessage},
};
use futures::{FutureExt, future};
use std::{
    collections::HashMap,
    fmt::format,
    net::SocketAddr,
    ops::Index,
    sync::Arc,
    time::{Duration, Instant},
    vec,
};
use tokio::{
    net::UdpSocket,
    runtime::Handle,
    sync::{
        Mutex,
        mpsc::{Receiver, Sender},
    },
    task::JoinError,
};

/// defines the max size a packet can be total
const MAX_PACKET_SIZE: usize = 1452;

/// a struct representing a packet that has only information needed for it to be sent directly to
/// the udp socket, with id to be able to report back failures, and duplicate count to send
/// multiple times in case of a control packet needing of redundancy
#[derive(Debug, Clone)]
pub struct SendablePacket {
    pub id: PacketId,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}

impl From<ProcessedPacket> for SendablePacket {
    /// simply copy the or move the needed values and discard those that are not relavent.
    fn from(value: ProcessedPacket) -> Self {
        Self {
            id: value.packet_id,
            data: value.data,
            duplicate_count: value.duplicate_count,
        }
    }
}

// Enum of errors that can occure relating to transport. includes internal for ergonomics.
#[derive(Debug, Clone)]
pub enum TransportError {
    CouldNotSend(Vec<PacketId>),
    FaildToBind,
    Internal(InternalError),
}

/// This might get more complex in the future. for now, checking this should be enough
impl PartialEq for TransportError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CouldNotSend(_), Self::CouldNotSend(_)) => true,
            (Self::FaildToBind, Self::FaildToBind) => true,
            _ => false,
        }
    }
}

/// The initializing function for the transport layer.
/// gets a reciver and sender for two seperate channels, one for receiving packets and commands from the packet
/// processor, and one for sending the packet processor received packets or propegating errors up.
/// also receives the port to bind the listening socket to.
/// The function returns only upon cleanup or if an error occured during setup.
pub async fn init(
    port: u16,
    mut receiver: Receiver<TransportSendMessage>,
    mut sender: Sender<Result<SendablePacket, TransportError>>,
) -> Result<(), TransportError> {
    let mut recv_handle = tokio::spawn(recv(port, sender.clone()));
    let mut send_handle = tokio::spawn(initialize_send(receiver));

    loop {
        let res = tokio::select! {
            res = &mut recv_handle, if recv_handle.is_finished() => {
                if let Ok(result) = recv_handle.await {
                    match result {
                        Ok(_) => {
                            send_handle.await;
                            return Ok(());
                        },
                        Err(err) => match err {
                            TransportError::
                        }
                    }
                }
            },
        _ = &mut send_handle => {}
        }

        //if recv_handle.is_finished() {
        //    if let Ok(res) = recv_handle.await {
        //        match res {
        //            Ok(_) => {
        //                _ = send_handle.await;
        //                return Ok(());
        //            }
        //            Err(err) => sender.send(Err(err)),
        //        };
        //    };
        //    recv_handle = tokio::spawn(recv(port, sender.clone()));
        //}
        //if send_handle.is_finished() {
        //    if let Ok(res) = send_handle.await {
        //        match res {
        //            Ok(_) => {
        //                recv_handle.abort();
        //                return Ok(());
        //            }
        //            Err((err, returned_receiver)) => {
        //                if err != TransportError::FaildToBind {
        //                    sender.send(Err(err));
        //                }
        //                receiver = returned_receiver;
        //            }
        //        }
        //        send_handle = tokio::spawn(initialize_send(receiver));
        //    } else {
        //        return Err(TransportError::Internal(InternalError::TaskFailed));
        //    }
        //}
    }

    Ok(())
}

/// this function is responsible for creating and maintaining the listening socket.
/// it gets as parameters the port to bind to, and a sender of a channel.
/// It should never realistically return Ok(()), but might return an error upon unrecoverable
/// failure.
/// This function will be forcefully aborted upon cleanup
pub async fn recv(
    port: u16,
    mut sender: Sender<Result<SendablePacket, TransportError>>,
) -> Result<(), TransportError> {
    let socket = Arc::new(
        UdpSocket::bind(format!("0.0.0.0:{port}"))
            .await
            .map_err(|_| TransportError::FaildToBind)?,
    );

    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];
        let (addr, stripped_buffer) = loop {
            if let Ok((read, addr)) = socket.recv_from(&mut buffer).await {
                break (addr, &buffer[..read]);
            }
        };

        let packet = SendablePacket {
            id: PacketId {
                timestamp: 0,
                session_token: get_session_token(addr),
            },
            data: Vec::from(stripped_buffer),
            duplicate_count: 0,
        };
        if let Err(intern) = send_to_processing_layer(sender.clone(), packet).await {
            return Err(TransportError::Internal(intern));
        }
    }
}

/// responsible for sending to the packet processor layer asyncronously.
/// gets as parameters a mutable reference to the sender and the packet to send up.
/// if the channel is closed, returns error immediately without blocking.
/// otherwise blocks until the data is sent on the channel or an error is returned.
pub async fn send_to_processing_layer(
    sender: Sender<Result<SendablePacket, TransportError>>,
    res: SendablePacket,
) -> Result<(), InternalError> {
    if sender.is_closed() {
        return Err(InternalError::ChannelClosed);
    }

    let res: Result<SendablePacket, TransportError> = Ok(res);

    if sender.send(res.clone()).await.is_err() {
        return Err(InternalError::ChannelFailed);
    } else {
        return Ok(());
    }
}

/// starts the send process, this will keep waiting until the channel raises an error or the Close
/// message is received.
/// it gets the receiver as an argument, and returns Ok(()) or Err((TransportError,
/// Receiver<...>)), the receiver cannot be borrowed or clones so it is moved back to the caller to
/// make sure the connection is not lost.
async fn initialize_send(
    mut receiver: Receiver<TransportSendMessage>,
) -> Result<(), (TransportError, Receiver<TransportSendMessage>)> {
    // loop forever
    loop {
        // buffer of tasks, processed periodically
        let mut tasks = vec![];
        let mut now = Instant::now();
        // exit this loop every 25 millis
        while now.elapsed() < Duration::from_millis(25) {
            // block until message received
            let Some(message) = receiver.recv().await else {
                return Err((
                    TransportError::Internal(InternalError::ChannelFailed),
                    receiver,
                ));
            };

            // act based on message
            match message {
                TransportSendMessage::Data(buffer) => tasks.push(tokio::spawn(send(buffer))),
                TransportSendMessage::Close => {
                    // ignore erros, just make sure to clean up
                    _ = futures::future::join_all(tasks);
                    return Ok(());
                }
            }
        }

        // periodic join to collect errors
        let results: Vec<_> = futures::future::join_all(tasks)
            .await
            .iter()
            .map(|res| res.as_ref().unwrap_or(&Ok(())))
            .filter(|res| res.is_err())
            .flat_map(|err| match err {
                Ok(()) => unreachable!(),
                Err(e) => match e {
                    TransportError::CouldNotSend(packet_id) => packet_id,
                    _ => unreachable!(),
                },
            })
            .map(|refr| *refr)
            .collect();

        if !results.is_empty() {
            return Err((TransportError::CouldNotSend(results), receiver));
        }
    }
}

/// internal send function, executed per buffer received.
/// it takes a buffer of ProcessedPackets, sorts by session, and sends concurrently with a task per session.
/// All tasks are joined in the end and any failed packets are collected to one TransportError.
pub async fn send(buffer: Vec<ProcessedPacket>) -> Result<(), TransportError> {
    // sort packets by session
    let mut sessions: HashMap<u128, Vec<SendablePacket>> = HashMap::new();
    for packet in buffer {
        let tok = packet.packet_id.session_token;
        let converted_packet: SendablePacket = SendablePacket::from(packet);
        sessions.entry(tok).or_default().push(converted_packet);
    }

    // try to create socket
    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        return Err(TransportError::FaildToBind);
    };

    // initiate futures
    let mut futures: Vec<_> = Vec::new();
    for (session, buffer) in sessions {
        futures.push(send_to(&socket, session, buffer));
    }

    // await all futures and save errors
    let results = futures::future::join_all(futures).await;
    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();

    // return Ok(()) if no errors occured
    // otherwise flat_map errors to one vector and return
    if errors.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CouldNotSend(
            errors
                .iter()
                .flat_map(|es| {
                    if let TransportError::CouldNotSend(val) = es {
                        val.clone()
                    } else {
                        Vec::new()
                    }
                })
                .collect::<Vec<PacketId>>(),
        ))
    }
}

/// atomic send operation
async fn send_to(
    socket: &UdpSocket,
    session_token: u128,
    buffer: Vec<SendablePacket>,
) -> Result<(), TransportError> {
    let mut errors: Vec<PacketId> = vec![];

    for packet in buffer {
        for _ in 0..packet.duplicate_count {
            if socket
                .send_to(&packet.data, get_addr(session_token))
                .await
                .is_err()
            {
                errors.push(packet.id);
            };
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TransportError::CouldNotSend(errors))
    }
}

/// blackbox placeholder for manage owned functions
pub fn get_addr(session_token: u128) -> String {
    let port = session_token / (12 * 100_000_012);
    format!("127.0.0.1:{port}")
}

/// blackbox placeholder for manage owned functions
pub fn get_session_token(addr: SocketAddr) -> u128 {
    (addr.port() as u128) * 12 * 100_000_012
}
