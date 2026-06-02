mod core;
mod rust;
mod types;
mod uniffi;

pub use core::{
    AppEvent, Connection, ConnectionEvent, IncomingConnection, PendingConnection, PendingStream,
    RequestedStream,
};
pub use types::{
    ApprovalStatus, Connection as ConnectionTrait, IncomingConnection as IncomingConnectionTrait,
    Input, Output, PendingConnection as PendingConnectionTrait,
    PendingStream as PendingStreamTrait, PlaybackControl, ReadableBuffer,
    RequestedStream as RequestedStreamTrait, Stream as StreamTrait, WriteableBuffer,
};

#[cfg(feature = "rust-api")]
pub use rust::*;

#[cfg(feature = "uniffi-api")]
pub use uniffi::*;
