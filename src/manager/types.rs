use std::net::SocketAddr;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::Instant,
};

use crate::prelude::*;

// processor channels
pub type OutboundSender = Sender<PacketProcessingMessage>;
pub type InboundReceiver = Receiver<Result<ManagerMessage>>;

// api channels
pub type InboundSender = Sender<Result<AppMessage>>;
pub type OutboundReceiver = Receiver<Result<ManagerMessage>>;

#[derive(Clone, Copy)]
pub struct Address(SocketAddr);

#[derive(Serialize, Clone, Copy, PartialEq, Debug)]
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// returns the current time since `PROTOCOL_EPOCH`
    ///
    /// # Panics
    /// This function panics if `PROTOCOL_EPOCH` is not yet initialized - an invariant
    pub fn now() -> Self {
        #[allow(clippy::cast_possible_truncation)]
        Self(
            Instant::now()
                .duration_since(*PROTOCOL_EPOCH.get().expect(
                    "Invariant broken while constructing `Timestamp`: \
        `PROTOCOL_EPOCH` is not initialized",
                ))
                .as_millis() as u64,
        )
    }
    pub fn set_again(&mut self) {
        *self = Timestamp::now();
    }

    pub fn been_longer_than(&self, millis: u64) -> bool {
        Self::now().0 - self.0 > millis
    }

    pub fn none() -> Self {
        Timestamp(0)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}
