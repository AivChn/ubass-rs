use crate::packet_processor::types::PacketId;
pub type Result<T> = core::result::Result<T, Error>;
pub type ErrResult = Result<()>;

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
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorContents {
    #[error("Channel: {0:?}")]
    Channel(ChannelError),
    #[error("Task: {0:?}")]
    Task(TaskError),
    #[error("Transport Error: {0:?}")]
    Transport(TransportError),
}

#[derive(Debug)]
pub enum TaskError {
    TaskFailed,
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
            any => Self::new(Recoverabilty::Recoverable, ErrorContents::Channel(any)),
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
