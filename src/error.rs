use crate::{
    packet_processor::types::PacketId,
    packetizer::types::{PacketType, SecondaryType, SessionId, Version},
};
pub type Result<T> = core::result::Result<T, Error>;
pub type ErrResult = Result<()>;
pub type EmptyResult = core::result::Result<(), ()>;

pub use PipeDirection::*;
pub use Recoverabilty::*;

#[derive(Debug)]
pub enum Recoverabilty {
    Recoverable,
    Unrecoverable,
}

#[derive(Debug)]
pub struct Error {
    recoverable: Recoverabilty,
    contents: ErrorContents,
}

impl Error {
    #[must_use]
    pub fn new(recoverable: Recoverabilty, contents: ErrorContents) -> Self {
        Self {
            recoverable,
            contents,
        }
    }

    pub fn is_recoverable(&self) -> bool {
        match self.recoverable {
            Recoverabilty::Recoverable => true,
            Recoverabilty::Unrecoverable => false,
        }
    }

    pub fn contents(&self) -> &ErrorContents {
        &self.contents
    }

    pub fn consume_contents(self) -> ErrorContents {
        self.contents
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorContents {
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
        Self {
            recoverable: Unrecoverable,
            contents: ErrorContents::Task(value),
        }
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
        match value {
            ChannelError::ChannelClosed(_) => {
                Self::new(Recoverabilty::Unrecoverable, ErrorContents::Channel(value))
            }
            any @ ChannelError::ChannelFailed(_) => {
                Self::new(Recoverabilty::Recoverable, ErrorContents::Channel(any))
            }
        }
    }
}

/// Errors that can occur during packet processing operations.
/// Covers deserialization failures, version incompatibilities, and internal errors.
#[derive(Debug)]
pub enum PacketProcessingError {
    IncompatibleVersion(Version),
    WrongHeaderSize(usize),
    InvalidPacketTypeHeader(u8),
    FailedToDeserialize,
}

impl From<PacketProcessingError> for Error {
    fn from(value: PacketProcessingError) -> Self {
        Self {
            recoverable: Unrecoverable,
            contents: ErrorContents::PacketProcessor(value),
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
}

impl From<TransportError> for Error {
    fn from(value: TransportError) -> Self {
        match value {
            TransportError::CouldNotSend(_) => Error {
                recoverable: Recoverabilty::Recoverable,
                contents: ErrorContents::Transport(value),
            },
            TransportError::RecvFailedTooManyTimes | TransportError::FailedToBind => Error {
                recoverable: Recoverabilty::Unrecoverable,
                contents: ErrorContents::Transport(value),
            },
        }
    }
}

impl TryFrom<Error> for TransportError {
    type Error = ();

    fn try_from(value: Error) -> core::result::Result<Self, Self::Error> {
        if let ErrorContents::Transport(err) = value.contents {
            Ok(err)
        } else {
            Err(())
        }
    }
}
