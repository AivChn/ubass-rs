#![allow(unused)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::must_use_candidate)]

pub mod error;
pub mod manager;
pub mod packet_processor;
pub mod packetizer;
pub mod prelude;
pub mod transport;
pub mod utils;

// Enum of possible internal protocol erros.
// These errors are specifically:
//  1. completely private - nothing outside the protocol itself can see them
//  2. inward pointing - only errors that cannot be pointed to outside causes (like I/O) are
//     considered internal
#[derive(Debug, Clone)]
pub enum InternalError {
    TaskFailed,
    ChannelFailed,
    ChannelClosed,
}
