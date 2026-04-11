use std::net::SocketAddr;

use crate::manager::packets::{BatchID, SessionId, Version};
pub type Result<T> = core::result::Result<T, Error>;
pub type ErrResult = Result<()>;
pub type EmptyResult = core::result::Result<(), ()>;

pub use PipeDirection::*;
use derive_more::Display;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Channel: {0}")]
    Channel(ChannelError),
    #[error("Task: {0}")]
    Task(TaskError),
    #[error("Transport Error: {0}")]
    Transport(TransportError),
    #[error("Packet Processing Error: {0}")]
    PacketProcessor(PacketProcessingError),
}

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("Async task failed to finish properly")]
    TaskFailed,
}

impl From<TaskError> for Error {
    fn from(value: TaskError) -> Self {
        Self::Task(value)
    }
}

#[derive(Debug, Display)]
pub enum PipeDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("{0} channel has failed")]
    ChannelFailed(PipeDirection),
    #[error("{0} channel has closed unexpectedly")]
    ChannelClosed(PipeDirection),
}

impl From<ChannelError> for Error {
    fn from(value: ChannelError) -> Self {
        Self::Channel(value)
    }
}

/// Errors that can occur during packet processing operations.
/// Covers deserialization failures, version incompatibilities, and internal errors.
#[derive(Debug, thiserror::Error)]
pub enum PacketProcessingError {
    #[error("Received a packet with an incompatible version ({0}) from {1}")]
    IncompatibleVersion(Version, SocketAddr),
    #[error("Got a packet with an impossible header size {0} from {1}")]
    WrongHeaderSize(usize, SocketAddr),
    #[error("Got a packe with an invalid packet type header ({0}")]
    InvalidPacketTypeHeader(u8),
    #[error("Faild to deserialize a packet")]
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
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Failed to bind a socket")]
    FailedToBind,
    #[error("Receiving failed too many times")]
    RecvFailedTooManyTimes,
}

impl From<TransportError> for Error {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}
