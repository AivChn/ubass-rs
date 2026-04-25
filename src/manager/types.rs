use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::Instant,
};

use crate::prelude::*;

// processor channels
pub type ManagerToProcessor = Sender<PacketProcessingMessage>;
pub type ManagerFromProcessor = Receiver<Result<ManagerMessage>>;

// api channels
pub type ManagerToApi = Sender<Result<ApiMessage>>;
pub type ManagerFromApi = Receiver<ApiCommand>;

#[derive(Debug, Clone, Copy)]
pub struct Address(SocketAddr);

#[derive(Serialize, Clone, Copy, PartialEq, Debug)]
#[repr(transparent)]
pub struct Timestamp(pub u64);

impl From<&ForeignTimestamp> for Timestamp {
    fn from(value: &ForeignTimestamp) -> Self {
        Self(value.get())
    }
}

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

    #[must_use]
    pub fn been_longer_than(&self, millis: u64) -> bool {
        Self::now().0 - self.0 > millis
    }

    #[must_use]
    pub fn none() -> Self {
        Timestamp(0)
    }

    #[must_use]
    pub fn get(&self) -> u64 {
        self.0
    }
}

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct ForeignTimestamp(AtomicU64);

impl ForeignTimestamp {
    #[must_use]
    pub fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }

    pub fn update(&mut self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }

    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
