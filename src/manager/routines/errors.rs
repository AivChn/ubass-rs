use crate::{
    get_state,
    manager::{
        STATE, routines::received::received_packet_with_incompatible_version,
        types::ManagerToProcessor,
    },
    prelude::*,
};
use tracing::{instrument, warn};

use ChannelError::{ChannelClosed, ChannelFailed};
use PacketProcessingError::IncompatibleVersion;
use TaskError::TaskFailed;
use TransportError::{FailedToBind, RecvFailedTooManyTimes};

#[instrument(skip_all)]
pub async fn handle_errors(error: Error, sender: ManagerToProcessor) {
    dbg!(&error);
    match error {
        Error::Task(TaskFailed) => {
            warn!("internal task died; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::Channel(ChannelFailed(direction, layer)) => {
            warn!("{direction} channel to layer {layer} failed; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::Channel(ChannelClosed(direction, layer)) => {
            warn!(
                "{direction} channel from layer {layer} closed unexpectedly; advertising protocol close"
            );
            get_state!().advertise_closed().await;
        }
        Error::Transport(FailedToBind) => {
            warn!(
                "transport reported FailedToBind mid-protocol (expected only at startup); advertising protocol close"
            );
            get_state!().advertise_closed().await;
        }
        Error::Transport(RecvFailedTooManyTimes) => {
            warn!("transport recv loop gave up; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::PacketProcessor(IncompatibleVersion(_, src_addr)) => {
            received_packet_with_incompatible_version(src_addr, sender.clone()).await;
        }
        Error::StateMismatch { expected, found } => {
            warn!(
                "state mismatch reached the error pipeline (expected {expected}, found {found}) — swallowing"
            );
        }
        Error::IrrelevantError => {
            warn!("IrrelevantError reached the error pipeline — swallowing");
        }
        e @ Error::FailedToDeref => {
            warn!("{e}");
        }
    }
}

// Dead code, kept as a placeholder for an eventual per-arm specific shutdown
// handler (the explicit-error API surface change tracked separately). All
// arms now log + advertise_closed instead of calling this.
#[allow(dead_code)]
fn panicking_error(error: &Error) -> ! {
    panic!("This error caused a panic - this would not happen in a final build.\n error: {error}")
}
