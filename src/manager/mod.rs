mod inbound;
mod key_exchange;
mod outbound;
pub mod packets;
pub mod types;

use crate::prelude::*;

use tokio::time::Instant;

pub fn init() {
    PROTOCOL_EPOCH.get_or_init(Instant::now);
}
