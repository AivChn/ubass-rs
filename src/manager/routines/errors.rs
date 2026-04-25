use crate::{
    manager::{routines::received::received_incompatible_version_error, types::ManagerToProcessor},
    prelude::*,
};

use ChannelError::{ChannelClosed, ChannelFailed};
#[allow(clippy::enum_glob_use)]
use PacketProcessingError::*;
use TaskError::TaskFailed;
use TransportError::{FailedToBind, RecvFailedTooManyTimes};

pub async fn handle_errors(error: Error, sender: ManagerToProcessor) {
    dbg!(&error);
    match error {
        error @ (Error::Task(TaskFailed)
        | Error::Channel(ChannelFailed(_) | ChannelClosed(_))
        | Error::Transport(FailedToBind | RecvFailedTooManyTimes)) => panicking_error(&error),
        Error::PacketProcessor(WrongHeaderSize(size, source)) => {}
        Error::PacketProcessor(InvalidPacketTypeHeader(value)) => {}
        Error::PacketProcessor(FailedToDeserialize) => {}
        Error::PacketProcessor(IncompatibleVersion(version, src_addr)) => {
            received_incompatible_version_error(version, src_addr, sender.clone()).await;
        }
    }
}

fn panicking_error(error: &Error) -> ! {
    panic!("This error caused a panic - this would not happen in a final build.\n error: {error}")
}
