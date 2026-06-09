use crate::{
    manager::{AppId, packets::*},
    prelude::*,
    utils::messages::{PacketProcessingMessage, TransportMessage},
};
use std::{fmt::Debug, net::SocketAddr, sync::Arc};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{Receiver, Sender};

/// Maximum UBASS packet size in bytes.
pub const MAX_PACKET_SIZE: usize = 1500 - 60 - 8;

// Check if all packets are below max size. Packets with no payload dont have an accurate
// calculation yet, since they are guaranteed to be smaller than ones with a payload, and those are
// accurate.
const _SIZES_FIT: () = {
    assert!(
        size_of::<HelloPacket>() + size_of::<Reserved<2>>() + AppId::MAX_LENGTH <= MAX_PACKET_SIZE
    );
    assert!(
        size_of::<TrackRequestPacket>() + size_of::<Reserved<2>>() + MAX_PAYLOAD_LENGTH
            <= MAX_PACKET_SIZE
    );
    assert!(size_of::<DataPacket>() + MAX_PAYLOAD_LENGTH <= MAX_PACKET_SIZE);
    assert!(size_of::<MetadataPacket>() + MAX_PAYLOAD_LENGTH <= MAX_PACKET_SIZE);
    assert!(size_of::<ParityPacket>() + ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH <= MAX_PACKET_SIZE);
    assert!(size_of::<AckPacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<KeepAlivePacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<HandshakeAckPacket>() <= MAX_PACKET_SIZE);
    assert!(
        size_of::<RetransmitPacket>() + RetransmitPacket::LOCAL_MAX_PAYLOAD_LENGTH
            <= MAX_PACKET_SIZE
    );
    assert!(size_of::<PlaybackControlPacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<IncompatibleVersionPacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<SessionDoesNotExistErrorPacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<UnexpectedPacketErrorPacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<TrackRejectionPacket>() + MAX_PAYLOAD_LENGTH <= MAX_PACKET_SIZE);
    assert!(size_of::<CloseSessionPacket>() <= MAX_PACKET_SIZE);
    assert!(size_of::<HandshakeRejection>() <= MAX_PACKET_SIZE);
};

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

/// A struct to manage outbound sockets, using a socket pool structure that expands and
/// contracts automatically based on throughput
pub struct OutboundSockets {
    sockets: Vec<Arc<UdpSocket>>,
    current_socket: usize,
    removal_meter: u64,
    addition_meter: u64,
    // number of batches that were sent earlier than timeout in a row
    early_batches: u64,
    // number of batches that were sent because of the timeout in a row
    on_time_batches: u64,
}

impl OutboundSockets {
    /// Customizeable thresholds
    const BUFFERS_BEFORE_REMOVE: u64 = 100;
    const BUFFERS_BEFORE_ADD: u64 = 50;
    // the formula for the sum of squares from 1..=n: n*(n+1)*(2n+1)/6
    const REMOVAL_THRESHOLD: u64 = Self::BUFFERS_BEFORE_REMOVE
        * (Self::BUFFERS_BEFORE_REMOVE + 1)
        * (2 * Self::BUFFERS_BEFORE_REMOVE + 1)
        / 6;
    // the threshold is decided based on a worst case scenario simulation of elapsed == 0 for every
    // single buffer in a row.
    const ADDITION_THRESHOLD: u64 = const {
        let mut acc = 0;
        let mut i = 1;
        let timeout_sq = BUFFER_TIMEOUT * BUFFER_TIMEOUT;

        while i <= Self::BUFFERS_BEFORE_ADD {
            acc += (i - 1) * timeout_sq / i;
            i += 1;
        }

        acc
    };

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
            removal_meter: 0,
            addition_meter: 0,
            early_batches: 0,
            on_time_batches: 0,
        })
    }

    /// swaps the current socket to the next one
    fn next_socket(&mut self) {
        // wrap to keep value within viable indeces
        self.current_socket = (self.current_socket + 1) % self.sockets.len();
    }

    /// Returns the current socket as an Arc
    #[must_use]
    pub fn retrieve(&mut self) -> Arc<UdpSocket> {
        // get socket
        let socket = self.sockets[self.current_socket].clone();
        // move current_socket
        self.next_socket();
        socket
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
            // decrease early by one - trick needed to make sure value is at minimum 1
            self.early_batches = self.early_batches.saturating_sub(2) + 1;
            self.on_time_batches += 1;
        } else {
            // vice versa
            self.on_time_batches = self.on_time_batches.saturating_sub(1);
            self.early_batches += 1;
        }

        // 0 increase if the relevant batch counter just reset
        self.removal_meter += self.on_time_batches * self.on_time_batches;
        // same as n^2 - (n^2 / early_batches)
        self.addition_meter += ((self.early_batches - 1) * (n * n)) / self.early_batches;

        // remove event triggered
        if self.removal_meter >= Self::REMOVAL_THRESHOLD {
            self.removal_meter = 0;
            if self.sockets.len() > 1 {
                // remove the next socket
                self.sockets
                    .remove((self.current_socket + 1) % self.sockets.len());
            }
        }

        // add event triggered
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
}
