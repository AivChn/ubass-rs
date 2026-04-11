use std::sync::Arc;

use crate::{
    dispatch,
    manager::types::{InboundReceiver, InboundSender},
    prelude::*,
};

pub async fn init(mut inbound_receiver: InboundReceiver) -> ErrResult {
    let monitor = Arc::new(HandleMonitor::default());
    monitor.clone().init();

    // loop
    loop {
        // receive
        let received = inbound_receiver.recv().await;
        // match
        let message = match received {
            // TODO: error handling
            None => todo!("error handling"),
            Some(Err(error)) => todo!("error handling"),
            // this message doesnt require any meaningful handling but should close this pipeline
            Some(Ok(ManagerMessage::Closed)) => return Ok(()),
            Some(Ok(message)) => message,
        };

        // dispatch
        dispatch!(handle_message(message) => monitor);
    }
}

async fn handle_message(message: ManagerMessage) {
    // TODO: dispatch routines
    match message {
        ManagerMessage::Recovered(recoverd_packets) => {
            todo!("recovered packets routine")
            // TODO: call recoverd routine
            // TODO: call data received routine
        }
        ManagerMessage::Packet(packet_wrapper) => {
            todo!("received packet routine")
        }
        ManagerMessage::Closed => unreachable!("This arm is handled in the `init` match"),
    }
}
