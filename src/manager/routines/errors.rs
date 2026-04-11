use crate::{
    manager::{routines::received::received_incompatible_version_error, types::OutboundSender},
    prelude::*,
};

use ChannelError::*;
use PacketProcessingError::*;
use TaskError::*;
use TransportError::*;

pub async fn handle_errors(error: Error, sender: OutboundSender) {
    dbg!(&error);
    match error {
        error @ (Error::Task(TaskFailed)
        | Error::Channel(ChannelFailed(_))
        | Error::Channel(ChannelClosed(_))
        | Error::Transport(FailedToBind)
        | Error::Transport(RecvFailedTooManyTimes)) => panicking_error(error),
        Error::PacketProcessor(WrongHeaderSize(size, source)) => {}
        Error::PacketProcessor(InvalidPacketTypeHeader(value)) => {}
        Error::PacketProcessor(FailedToDeserialize) => {}
        Error::PacketProcessor(IncompatibleVersion(version, src_addr)) => {
            received_incompatible_version_error(version, src_addr, sender.clone()).await;
        }
    }
}

fn panicking_error(error: Error) -> ! {
    panic!("This error caused a panic - this would not happen in a final build.\n error: {error}")
}
