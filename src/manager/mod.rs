mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
mod state;
pub mod types;

use crate::prelude::*;

use tokio::time::Instant;

pub use state::{AppId, EncryptionMonitor, FingerprintMonitor, PendingAckMonitor};

pub fn init() {
    PROTOCOL_EPOCH.get_or_init(Instant::now);
}

fn initiate_handshake() -> Result<i32> {
    todo!()
}
