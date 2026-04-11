use crate::prelude::*;

use ChannelError::*;
use PacketProcessingError::*;
use TaskError::*;
use TransportError::*;

pub fn handle_errors(error: Error) {
    match error {
        Error::Task(TaskFailed) => todo!(),
        Error::Channel(ChannelFailed(direction)) => todo!(),
        Error::Channel(ChannelClosed(direction)) => todo!(),
        Error::Transport(FailedToBind) => todo!(),
        Error::Transport(RecvFailedTooManyTimes) => todo!(),
        Error::PacketProcessor(IncompatibleVersion(version, socket_addr)) => todo!(),
        Error::PacketProcessor(RecoveryNotReady(session_id, batch_id)) => todo!(),
        Error::PacketProcessor(WrongHeaderSize(size)) => todo!(),
        Error::PacketProcessor(InvalidPacketTypeHeader(value)) => todo!(),
        Error::PacketProcessor(FailedToDeserialize) => todo!(),
    }
}
