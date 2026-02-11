pub mod packet_processor;
pub mod packetizer;
pub mod transport;

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
