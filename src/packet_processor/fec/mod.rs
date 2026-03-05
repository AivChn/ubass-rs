mod reed_solomon;
mod xor;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::packetizer::types::ParityPacket;
use crate::prelude::*;

use crate::{
    packet_processor::{Batch, FecPacket},
    packetizer::types::DataPacket,
};

struct OutboundBatch {
    packets: Vec<FecPacket>,
    batch_id: u16,
    batch_size: u8,
    batch_top: u8,
}

impl OutboundBatch {
    fn new() -> Self {
        Self {
            packets: Vec::new(),
            batch_id: Self::get_batch_id(),
            batch_size: Self::get_batch_size(),
            batch_top: 0,
        }
    }

    fn get_batch_id() -> u16 {
        // ignore this i just felt like it
        (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("TIME")
            .as_millis()
            * 3_432_141_324) as u16
            ^ ((69_567_564_845 >> 33) & 2_134_123_512) as u16
    }

    fn get_batch_size() -> u8 {
        24
    }
}

enum FECType {
    RS,
    XOR,
}

const CURRENT_TYPE: FECType = FECType::RS;

#[inline(always)]
pub async fn received(batch: Batch, pack: FecPacket) -> bool {
    match CURRENT_TYPE {
        FECType::RS => reed_solomon::received(batch, pack).await,
        FECType::XOR => xor::received(batch, pack).await,
    }
}

#[inline(always)]
pub async fn sent(packet: DataPacket) -> Option<ParityPacket> {
    match CURRENT_TYPE {
        FECType::RS => reed_solomon::sent(packet).await,
        FECType::XOR => xor::sent(packet).await,
    }
}

fn derive_parity(mut entry: OutboundBatch) -> Option<FecPacket> {}
