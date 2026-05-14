mod core;
#[cfg(feature = "data-collection-api")]
mod dc_proto;
#[cfg(feature = "data-collection-api")]
mod dc_receiver;
#[cfg(feature = "data-collection-api")]
mod dc_sender;
mod rust;
mod types;
mod uniffi;

pub use core::{
    AppEvent, Connection, ConnectionEvent, IncomingConnection, PendingConnection, PendingStream,
    RequestedStream,
};
pub use types::{
    ApprovalStatus, Connection as ConnectionTrait, IncomingConnection as IncomingConnectionTrait,
    PendingConnection as PendingConnectionTrait, PendingStream as PendingStreamTrait,
    PlaybackControl, ReadableBuffer, RequestedStream as RequestedStreamTrait,
    Stream as StreamTrait, WriteableBuffer,
};

#[cfg(feature = "rust-api")]
pub use rust::*;

#[cfg(feature = "uniffi-api")]
pub use uniffi::*;

#[cfg(feature = "data-collection-api")]
pub use dc_receiver::{MAX_BUFFER_LEN, Receiver};
#[cfg(feature = "data-collection-api")]
pub use dc_sender::{Sender, SenderOptions};
