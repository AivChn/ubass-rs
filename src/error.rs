use std::{io, net::SocketAddr};

use crate::{
    api::IncomingConnection,
    error,
    manager::{
        packets::{BatchID, SessionId, Version},
        state::{ConnectionStates, EstablishedState, SessionStates, StreamState, Streaming},
    },
};
pub type Result<T> = core::result::Result<T, Error>;
pub type ErrResult = Result<()>;
pub type EmptyResult = core::result::Result<(), ()>;

#[derive(Debug, thiserror::Error)]
pub enum ApiErrors {
    #[error("Protocol already opened - cannot open more than one instance per program")]
    AlreadyOpen,
    #[error("Cannot use ports 1024 or lower, please suggest a different port.")]
    InvalidPort,
    #[error("AppId is not a valid ID: its either too long or contains non ascii characters")]
    InvalidAppId,
    #[error("Cannot use port {0}, already in use.")]
    PortAlreadyInUse(u16),
    #[error("Failed to open the protocol")]
    FailedToOpen,
    #[error("Failed to build runtime: {0:?}")]
    FailedToBuildRuntime(io::Error),
    #[error("thread {0} failed at some point.")]
    ThreadFailed(&'static str),
    #[error("No free session available for the given target")]
    NoFreeSession,
    #[error("Session is occupied")]
    SessionOccupied,
    #[error("Session does not exist")]
    SessionDoesNotExist,
    #[error("Rejection reason must be valid ASCII and below MAX_PAYLOAD_LENGTH")]
    InvalidReason,
    #[error("Protocol is closed")]
    ProtocolClosed,
    #[error("Buffer exceeds MAX_PAYLOAD_LENGTH")]
    BufferTooLarge,
}

// this is a comment
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Protocol is closed")]
    ProtocolClosed,
    #[error("Buffer exceeds MAX_PAYLOAD_LENGTH")]
    BufferTooLarge,
    #[error("Session is occupied")]
    SessionOccupied,
    #[error("Session was closed by peer")]
    SessionClosedByPeer,
    #[error("This error should not happen")]
    UnknownInternalError,
    #[error("Reason is too long or not valid ASCII")]
    InvalidReason(IncomingConnection),
    #[error("peer rejected the handhsake: {}", .0.as_ref().unwrap_or(&"FAILED TO PARSE".to_string()))]
    PeerRejected(Option<String>),
    #[error("Congrats! the universe is gone and you got 2 u64 collisions (1 in 2^65 chance)")]
    SessionIdCollided,
}

impl ConnectionError {
    pub(crate) fn from_api(error: ApiErrors) -> Self {
        match error {
            ApiErrors::SessionOccupied => ConnectionError::SessionOccupied,
            ApiErrors::SessionDoesNotExist => ConnectionError::SessionClosedByPeer,
            ApiErrors::ProtocolClosed => ConnectionError::ProtocolClosed,
            ApiErrors::BufferTooLarge => ConnectionError::BufferTooLarge,
            e => {
                debug_assert!(
                    false,
                    "Invariant broken while converting ApiError to ConnectionError: \
                            got an invalid variant {e:?}: {e}"
                );
                ConnectionError::UnknownInternalError
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StreamErrors {
    #[error("{0}")]
    Connection(ConnectionError),
    #[error("Stream was paused by peer")]
    PausedByPeer,
}

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
    #[error("State did not match the expected state. expected {expected}, found {found} ")]
    StateMismatch {
        expected: FlatState,
        found: FlatState,
    },
}

#[derive(Debug, Display)]
pub enum FlatState {
    Up,
    Down,
    StreamingTo,
    StreamingFrom,
    Handshake,
}

impl From<&ConnectionStates> for FlatState {
    fn from(value: &ConnectionStates) -> Self {
        match value {
            ConnectionStates::Handshake { .. } => Self::Handshake,
            ConnectionStates::Established(box EstablishedState {
                state: SessionStates::Up,
                ..
            }) => Self::Up,
            ConnectionStates::Established(box EstablishedState {
                state: SessionStates::Down,
                ..
            }) => Self::Down,
            ConnectionStates::Established(box EstablishedState {
                state:
                    SessionStates::Streaming(StreamState {
                        streaming: Streaming::To(_),
                        ..
                    }),
                ..
            }) => Self::StreamingTo,
            ConnectionStates::Established(box EstablishedState {
                state:
                    SessionStates::Streaming(StreamState {
                        streaming: Streaming::From(_),
                        ..
                    }),
                ..
            }) => Self::StreamingFrom,
        }
    }
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

#[derive(Debug, Display, Clone, Copy)]
pub enum Layer {
    Manager,
    PacketProcessor,
    Transport,
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("{0} channel has failed")]
    ChannelFailed(PipeDirection, Layer),
    #[error("{0} channel has closed unexpectedly")]
    ChannelClosed(PipeDirection, Layer),
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
    #[error("Session does not exist on this host")]
    SessionDoesNotExist(SessionId, SocketAddr),
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
