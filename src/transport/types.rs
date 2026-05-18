use crate::{
    prelude::*,
    utils::messages::{PacketProcessingMessage, TransportMessage},
};
use std::{fmt::Debug, net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{Receiver, Sender};

/// Maximum UDP packet size in bytes (1452).
pub const MAX_PACKET_SIZE: usize = 1452;

pub const MAX_CONCURRENT_SENDS: u64 = 128;
pub const MAX_PACKET_BUFFER_SIZE: usize = 128;
pub const BUFFER_TIMEOUT: u64 = 5;

pub type OutboundReceiver = Receiver<TransportMessage>;
pub type InboundSender = Sender<Result<PacketProcessingMessage>>;

/// Packaging for the two channels the inbound task accesses
pub struct TransportChannels {
    pub receiver: OutboundReceiver,
    pub sender: InboundSender,
}

/// A packet straight from the socket oven
#[derive(Debug, Clone)]
pub struct ReceivedPacket {
    pub src_addr: SocketAddr,
    pub data: Vec<u8>,
}

/// A struct to manage outbound sockets, using a ring buffer esque structure that expands and
/// contracts automatically based on throughput
pub struct OutboundSockets {
    sockets: Vec<Arc<UdpSocket>>,
    current_socket: usize,
    swappinness_meter: u64,
    removal_meter: u64,
    addition_meter: u64,
    early_batches: u64,
    on_time_batches: u64,
}

impl OutboundSockets {
    /// Customizeable thresholds
    const SWAPPINNESS_THRESHOLD: u64 = 5_760;
    const REMOVAL_THRESHOLD: u64 = 54_651;
    const ADDITION_THRESHOLD: u64 = 26_224;

    /// Creates a new `OutboundSockets` struct
    ///
    /// # Errors
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
            swappinness_meter: 0,
            removal_meter: 0,
            addition_meter: 0,
            early_batches: 0,
            on_time_batches: 0,
        })
    }

    /// Returns the current socket as an Arc
    #[must_use]
    pub fn retrieve(&self) -> Arc<UdpSocket> {
        self.sockets[self.current_socket].clone()
    }

    /// Updates all the meters based on the time it took for a buffer to be sent.
    ///
    /// # Errors
    /// This function may erorr if creating a new socket has failed
    pub async fn update(&mut self, elapsed: u64) -> ErrResult {
        #[allow(clippy::cast_possible_truncation)]
        #[allow(clippy::cast_sign_loss)]
        // get the time left until the buffer would have been sent, 0 if the buffer was sent at the
        // last moment
        let n = BUFFER_TIMEOUT.saturating_sub(elapsed);
        if n == 0 {
            // if the buffer wasnt filled or filled just at the last second,
            // reset early batches and increase on time batches.
            self.early_batches = 1;
            self.on_time_batches += 1;
        } else {
            // vice versa
            self.on_time_batches = 1;
            self.early_batches += 1;
        }

        // calculate the meters
        self.swappinness_meter += n * n;
        // 0 increase if the relevant batch counter just reset
        self.removal_meter += (n * n) - ((n * n) / self.on_time_batches);
        self.addition_meter += (n * n) - ((n * n) / self.early_batches);

        if self.swappinness_meter >= Self::SWAPPINNESS_THRESHOLD {
            self.swappinness_meter = 0;
            self.next_socket();
        }

        if self.removal_meter >= Self::REMOVAL_THRESHOLD {
            self.removal_meter = 0;
            if self.current_socket == self.sockets.len() - 1 {
                self.next_socket();
            }
            self.sockets
                .remove((self.current_socket + 1) % self.sockets.len());
        }

        if self.addition_meter >= Self::ADDITION_THRESHOLD {
            self.addition_meter = 0;
            self.sockets.push(Self::new_socket().await?);
        }

        Ok(())
    }

    /// Adds a socket to the buffer
    ///
    /// # Errors
    /// can return a `FailedToBind` error if socket binding failed
    async fn new_socket() -> Result<Arc<UdpSocket>> {
        Ok(Arc::new(
            UdpSocket::bind("0.0.0.0:0")
                .await
                .map_err(|_| TransportError::FailedToBind)?,
        ))
    }

    /// swaps the current socket to the next one
    fn next_socket(&mut self) {
        self.current_socket = (self.current_socket + 1) % self.sockets.len();
    }
}
