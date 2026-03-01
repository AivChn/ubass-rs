use crate::{
    InternalError,
    packet_processor::types::{PacketId, ProcessedPacket, TransportSendMessage},
};
use std::fmt::Debug;
use tokio::sync::mpsc::{Receiver, Sender};

/// Maximum UDP packet size in bytes (1452).
pub const MAX_PACKET_SIZE: usize = 1452;

pub const MAX_CONCURRENT_SENDS: usize = 128;

pub struct InboundChannels {
    pub receiver: Receiver<TransportSendMessage>,
    pub sender: Sender<Result<ReceivedPacket, TransportError>>,
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

/// Errors that can occur in the transport layer.
///
/// These errors are used both internally for task supervision and externally
/// to communicate failures to the packet processor layer.
#[derive(Debug, Clone)]
pub enum TransportError {
    /// One or more packets failed to send. Contains the IDs of failed packets
    /// so they can be retried or reported by upper layers.
    CouldNotSend(Vec<PacketId>),
    /// Failed to bind a UDP socket to the requested address/port.
    FailedToBind,
    RecvFailedTooManyTimes,
    /// Internal protocol error (task failure, channel issues).
    /// Wraps [`InternalError`] for ergonomic error propagation.
    Internal(InternalError),
}

impl PartialEq for TransportError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::CouldNotSend(_), Self::CouldNotSend(_)) => true,
            (Self::FailedToBind, Self::FailedToBind) => true,
            _ => false,
        }
    }
}
