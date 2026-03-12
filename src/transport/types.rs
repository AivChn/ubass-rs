use crate::{
    packet_processor::types::{PacketId, ProcessedPacket, TransportMessage},
    prelude::*,
};
use std::{cmp::min, fmt::Debug, sync::Arc};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{Receiver, Sender};

/// Maximum UDP packet size in bytes (1452).
pub const MAX_PACKET_SIZE: usize = 1452;

pub const MAX_CONCURRENT_SENDS: usize = 128;
pub const MAX_PACKET_BUFFER_SIZE: usize = 128;
pub const BUFFER_TIMEOUT: u64 = 25;

pub struct InboundChannels {
    pub receiver: Receiver<TransportMessage>,
    pub sender: Sender<Result<ReceivedPacket>>,
}

pub struct OutboundSockets {
    sockets: Vec<Arc<UdpSocket>>,
    current_socket: usize,
    swappinness: u64,
    removal: u64,
    addition: u64,
    early_batches: u64,
    on_time_batches: u64,
}

impl OutboundSockets {
    const SWAPPINNESS_THRESHOLD: u64 = 5_760;
    const REMOVAL_THRESHOLD: u64 = 54_651;
    const ADDITION_THRESHOLD: u64 = 26_224;

    /// Creates a new `OutboundSockets` struct
    ///
    /// # Errors
    ///
    /// This function may erorr if it failed to create a socket.
    pub async fn new() -> Result<Self> {
        let sockets = vec![Arc::new(
            UdpSocket::bind("0.0.0.0:0")
                .await
                .map_err(|_| TransportError::FailedToBind)?,
        )];

        Ok(Self {
            sockets,
            current_socket: 0,
            swappinness: 0,
            removal: 0,
            addition: 0,
            early_batches: 0,
            on_time_batches: 0,
        })
    }

    pub fn retrieve(&self) -> Arc<UdpSocket> {
        self.sockets[self.current_socket].clone()
    }

    /// Updates all the meters based on the time it took for a buffer to be sent.
    ///
    /// # Errors
    ///
    /// This function may erorr if creating a new socket has failed
    pub async fn update(&mut self, elapsed: u64) -> ErrResult {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        let n = min(BUFFER_TIMEOUT as i128 - elapsed as i128, 0) as u64;
        if n == 0 {
            self.early_batches = 1;
            self.on_time_batches += 1;
        } else {
            self.on_time_batches = 1;
            self.early_batches += 1;
        }

        self.swappinness += n * n;
        self.removal += (n * n) - ((n * n) / self.on_time_batches);
        self.addition += (n * n) - ((n * n) / self.early_batches);

        if self.swappinness >= Self::SWAPPINNESS_THRESHOLD {
            self.swappinness = 0;
            self.next_socket();
        }

        if self.removal >= Self::REMOVAL_THRESHOLD {
            self.removal = 0;
            if self.current_socket == self.sockets.len() - 1 {
                self.next_socket();
            }
            self.sockets
                .remove((self.current_socket + 1) % self.sockets.len());
        }

        if self.addition >= Self::ADDITION_THRESHOLD {
            self.addition = 0;
            self.sockets.push(Self::new_socket().await?);
        }

        Ok(())
    }

    async fn new_socket() -> Result<Arc<UdpSocket>> {
        Ok(Arc::new(
            UdpSocket::bind("0.0.0.0:0")
                .await
                .map_err(|_| TransportError::FailedToBind)?,
        ))
    }

    fn next_socket(&mut self) {
        self.current_socket = (self.current_socket + 1) % self.sockets.len();
    }
}

/// Packet ready for UDP transmission with metadata for error reporting and redundancy.
#[derive(Debug, Clone)]
pub struct SendablePacket {
    pub id: PacketId,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct ReceivedPacket {
    pub data: Vec<u8>,
}

impl ReceivedPacket {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
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
