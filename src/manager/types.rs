use std::sync::atomic::{AtomicU64, Ordering};

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

/// Represents the number of ms since PROTOCOL_EPOCH at point of construction
#[derive(Serialize, Clone, Copy, PartialEq, Debug)]
#[repr(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// Returns the current time since `PROTOCOL_EPOCH`
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

    /// Set the timestamp to `Timestamp::now()` in place
    pub fn set_again(&mut self) {
        *self = Timestamp::now();
    }

    /// check if it has been longer than `millis` since the timestamp to now.
    #[must_use]
    pub fn been_longer_than(&self, millis: u64) -> bool {
        Self::now().0 - self.0 > millis
    }

    #[must_use]
    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<&ForeignTimestamp> for Timestamp {
    fn from(value: &ForeignTimestamp) -> Self {
        Self(value.get())
    }
}

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct ForeignTimestamp(AtomicU64);

impl ForeignTimestamp {
    pub fn update(&mut self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }

    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}
