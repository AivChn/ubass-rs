use tokio::time::Instant;

use crate::prelude::*;

pub async fn init() {
    PROTOCOL_EPOCH.get_or_init(Instant::now);
}
