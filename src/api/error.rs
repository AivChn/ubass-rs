use std::io;

#[derive(Debug, thiserror::Error)]
pub enum ApiErrors {
    #[error("Protocol already opened - cannot open more than one instance per program")]
    AlreadyOpen,
    #[error("Cannot use ports 1024 or lower, please suggest a different port.")]
    InvalidPort,
    #[error("Cannot use port {0}, already in use.")]
    PortAlreadyInUse(u16),
    #[error("Failed to build runtime: {0:?}")]
    FailedToBuildRuntime(io::Error),
    #[error("thread {0} failed at some point.")]
    ThreadFailed(&'static str),
}
