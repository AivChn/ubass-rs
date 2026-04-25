mod core;
mod rust;
mod types;
mod uniffi;

pub use core::{AppEvent, Connection, IncomingConnection, PendingConnection};
pub use types::{
    Connection as ConnectionTrait, IncomingConnection as IncomingConnectionTrait,
    PendingConnection as PendingConnectionTrait, ReadableBuffer, Stream as StreamTrait,
    WriteableBuffer,
};

#[cfg(feature = "rust-api")]
pub use rust::*;

#[cfg(feature = "rust-api")]
pub use uniffi::*;
