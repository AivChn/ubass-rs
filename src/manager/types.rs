use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use aes_gcm_siv::Aes256GcmSiv;
use tokio::{
    sync::{
        Mutex, RwLock,
        mpsc::{Receiver, Sender},
    },
    time::Instant,
};

use crate::{
    manager::packets::{PacketFingerprint, PacketWrapper, SessionId},
    packet_processor::serialize::Serialize,
    prelude::*,
};

type OutboundReceiver = Receiver<Result<ManagerMessage>>;
type OutboundSender = Sender<PacketProcessingMessage>;

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
