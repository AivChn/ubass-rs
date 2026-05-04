use crate::manager::{
    self,
    packets::{BatchID, FECInfo},
};

pub const MAGIC_BATCH_SIZE: usize = manager::state::PACKET_COUNT_PER_BATCH;

pub struct BatchInfo {
    batch_size: u8,
    recovery_count: u8,
}

#[must_use]
pub fn create_batch(packets_left: usize) -> BatchInfo {
    // this picks the minimum between any number and 28, it will be within the u8 range
    #[allow(clippy::cast_possible_truncation)]
    let batch_size = packets_left.min(MAGIC_BATCH_SIZE) as u8;
    BatchInfo {
        batch_size,
        recovery_count: 1,
    }
}
