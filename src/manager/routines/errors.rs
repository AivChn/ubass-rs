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
        // Fatal infrastructure errors. None of them are recoverable from the
        // manager's perspective — the layer that produced them is already
        // broken or torn down. Per-variant logs so the cause is visible;
        // advertise_closed bubbles ConnectionEvent::ProtocolClosed to every
        // active session before the channels finish unwinding.
        // (App-level error specificity — explicit reasons via api.listen() —
        // is a follow-up; today every arm sends the opaque ProtocolClosed.)
        Error::Task(TaskFailed) => {
            warn!("internal task died; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::Channel(ChannelFailed(direction, layer)) => {
            warn!("{direction} channel to layer {layer} failed; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::Channel(ChannelClosed(direction, layer)) => {
            warn!("{direction} channel from layer {layer} closed unexpectedly; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::Transport(FailedToBind) => {
            warn!("transport reported FailedToBind mid-protocol (expected only at startup); advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::Transport(RecvFailedTooManyTimes) => {
            warn!("transport recv loop gave up; advertising protocol close");
            get_state!().advertise_closed().await;
        }
        Error::PacketProcessor(IncompatibleVersion(_, src_addr)) => {
            received_packet_with_incompatible_version(src_addr, sender.clone()).await;
        }
        // Both of these are raised inside manager state operations and are
        // already handled at their call sites (StateMismatch in
        // `received_data_packet` triggers an `UnexpectedPacketErrorPacket`
        // response there; IrrelevantError is by-design swallowed at the
        // source). They aren't reachable from packet_processor errors today,
        // but the never-panic policy means we still want a sane handler in
        // case some future code path routes them here.
        Error::StateMismatch { expected, found } => {
            warn!("state mismatch reached the error pipeline (expected {expected}, found {found}) — swallowing");
        }
        Error::IrrelevantError => {
            warn!("IrrelevantError reached the error pipeline — swallowing");
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
