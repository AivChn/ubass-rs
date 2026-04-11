use std::net::SocketAddr;

use crate::manager::packets::{BatchID, SessionId, Version};
pub type Result<T> = core::result::Result<T, Error>;
pub type ErrResult = Result<()>;
pub type EmptyResult = core::result::Result<(), ()>;

pub use PipeDirection::*;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Channel: {0:?}")]
    Channel(ChannelError),
    #[error("Task: {0:?}")]
    Task(TaskError),
    #[error("Transport Error: {0:?}")]
    Transport(TransportError),
    #[error("Packet Processing Error: {0:?}")]
    PacketProcessor(PacketProcessingError),
}

#[derive(Debug)]
pub enum TaskError {
    TaskFailed,
}

impl From<TaskError> for Error {
    fn from(value: TaskError) -> Self {
        Self::Task(value)
    }
}

#[derive(Debug)]
pub enum PipeDirection {
    Inbound,
    Outbound,
}

#[derive(Debug)]
pub enum ChannelError {
    ChannelFailed(PipeDirection),
    ChannelClosed(PipeDirection),
}

impl From<ChannelError> for Error {
    fn from(value: ChannelError) -> Self {
        Self::Channel(value)
    }
}

/// Errors that can occur during packet processing operations.
/// Covers deserialization failures, version incompatibilities, and internal errors.
#[derive(Debug)]
pub enum PacketProcessingError {
    IncompatibleVersion(Version, SocketAddr),
    RecoveryNotReady(SessionId, BatchID),
    WrongHeaderSize(usize),
    InvalidPacketTypeHeader(u8),
    FailedToDeserialize,
}

impl From<PacketProcessingError> for Error {
    fn from(value: PacketProcessingError) -> Self {
        Self::PacketProcessor(value)
    }
}

/// Errors that can occur in the transport layer.
///
/// These errors are used both internally for task supervision and externally
/// to communicate failures to the packet processor layer.
#[derive(Debug, Clone)]
pub enum TransportError {
    /// Failed to bind a UDP socket to the requested address/port.
    FailedToBind,
    RecvFailedTooManyTimes,
}

impl From<TransportError> for Error {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}
