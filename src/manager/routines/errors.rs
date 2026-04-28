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
        Error::PacketProcessor(
            WrongHeaderSize(_, _) | InvalidPacketTypeHeader(_) | FailedToDeserialize,
        ) => {}
        Error::PacketProcessor(IncompatibleVersion(version, src_addr)) => {
            received_incompatible_version_error(version, src_addr, sender.clone()).await;
        }
        Error::StateMismatch { .. } => todo!(),
    }
}

fn panicking_error(error: &Error) -> ! {
    panic!("This error caused a panic - this would not happen in a final build.\n error: {error}")
}
