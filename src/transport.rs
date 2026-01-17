use futures::future;
use std::collections::HashMap;
use std::{io, sync::Arc};
use tokio::net::{ToSocketAddrs, UdpSocket};

use crate::{
    packet_processor::{PacketId, ProcessedPacket},
    packetizer::PacketType,
};

pub enum SendError {
    CouldNotSend(Vec<PacketId>),
    FaildToBind,
}

impl PartialEq for SendError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CouldNotSend(_), Self::CouldNotSend(_)) => true,
            (Self::FaildToBind, Self::FaildToBind) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
struct SendablePacket {
    id: PacketId,
    data: Vec<u8>,
    duplicate_count: usize,
}

impl From<ProcessedPacket> for SendablePacket {
    fn from(value: ProcessedPacket) -> Self {
        Self {
            id: value.packet_id,
            data: value.data,
            duplicate_count: value.duplicate_count,
        }
    }
}

impl SendablePacket {
    fn new(data: Vec<u8>, duplicate_count: usize, id: PacketId) -> Self {
        Self {
            id,
            data,
            duplicate_count,
        }
    }
}

fn get_addr(session_token: u128) -> String {
    _ = session_token;
    "127.0.0.1:6969".to_string()
}

async fn send_to(
    socket: &UdpSocket,
    session_token: u128,
    buffer: Vec<SendablePacket>,
) -> Result<(), SendError> {
    let mut errors: Vec<PacketId> = vec![];

    for packet in buffer {
        if socket
            .send_to(&packet.data, get_addr(session_token))
            .await
            .is_err()
        {
            errors.push(packet.id);
        };
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(SendError::CouldNotSend(errors))
    }
}

fn decide_dup(ty: PacketType) -> usize {
    1
}

pub fn recv() -> Result<(), ()> {
    Ok(())
}

pub async fn send(buffer: Vec<ProcessedPacket>) -> Result<(), SendError> {
    // sort packets by session
    let mut sessions: HashMap<u128, Vec<SendablePacket>> = HashMap::new();
    for packet in buffer {
        let tok = packet.packet_id.session_token;
        let converted_packet: SendablePacket = SendablePacket::from(packet);
        sessions.entry(tok).or_default().push(converted_packet);
    }

    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        return Err(SendError::FaildToBind);
    };

    // tokio_scoped::scope(|s| {
    //     for (tok, buffer) in sessions {
    //         let res: Result<(), Vec<PacketId>> = s.spawn(send_to(&socket, tok, buffer));
    //     }
    // });

    let mut futures: Vec<_> = Vec::new();

    for (session, buffer) in sessions {
        futures.push(send_to(&socket, session, buffer));
    }

    let results = futures::future::join_all(futures).await;

    let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(SendError::CouldNotSend(
            errors
                .iter()
                .flat_map(|es| {
                    if let SendError::CouldNotSend(val) = es {
                        val
                    } else {
                        &vec![]
                    }
                })
                .collect::<Vec<PacketId>>(),
        ))
    }
}
