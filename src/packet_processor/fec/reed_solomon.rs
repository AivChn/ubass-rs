use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tokio::time::Instant;

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use crate::{
    packet_processor::fec::FECPacket,
    packetizer::types::{
        BatchID, FECInfo, Options, PacketType, PacketTypeFecBatchID, ParityPacket, SessionId,
        Version,
    },
    prelude::*,
};

use super::*;

struct InboundBatchData {
    decoder: ReedSolomonDecoder,
    batch_size: u8,
    recovery_count: u8,
    received_count: u8,
}

impl From<&FECPacket> for Arc<Mutex<InboundBatchData>> {
    fn from(value: &FECPacket) -> Self {
        let batch_size = value.batch_size;
        let recovery_count = value.recovery_count;
        Arc::new(Mutex::new(InboundBatchData {
            decoder: ReedSolomonDecoder::new(
                batch_size as usize,
                recovery_count as usize,
                FEC_DATA_SIZE,
            )
            .expect("The values possible for a u8 are well within the limits"),
            batch_size,
            recovery_count,
            received_count: 0,
        }))
    }
}

struct OutboundBatchData {
    encoder: ReedSolomonEncoder,
    batch_size: u8,
    recovery_count: u8,
    received_count: u8,
}

impl From<&FECPacket> for Arc<Mutex<OutboundBatchData>> {
    fn from(value: &FECPacket) -> Self {
        let batch_size = value.batch_size;
        let recovery_count = value.recovery_count;
        Arc::new(Mutex::new(OutboundBatchData {
            encoder: ReedSolomonEncoder::new(
                batch_size as usize,
                recovery_count as usize,
                FEC_DATA_SIZE,
            )
            .expect("The values possible for a u8 are well within the limits"),
            batch_size,
            recovery_count,
            received_count: 0,
        }))
    }
}

impl From<(u8, &FECPacket, &[u8])> for ParityPacket {
    fn from(value: (u8, &FECPacket, &[u8])) -> Self {
        let (i, packet, payload) = value;
        ParityPacket::new(
            Box::new(payload.try_into().expect("Length is guaranteed by library")),
            Options::construct(Vec::with_capacity(0)),
            PacketTypeFecBatchID(PacketType::Parity, packet.batch_id),
            FECInfo {
                batch_size: packet.batch_size,
                batch_pos: i,
                recovery_size: packet.recovery_count,
            },
            packet.session_id,
            Instant::now()
                .duration_since(*PROTOCOL_EPOCH.get_or_init(Instant::now))
                .as_millis() as u64,
        )
    }
}

/// Represents the full state of the Reed-Solomon FEC strategy
pub struct RS {
    inbound: Mutex<HashMap<(BatchID, SessionId), Arc<Mutex<InboundBatchData>>>>,
    outbound: Mutex<HashMap<(BatchID, SessionId), Arc<Mutex<OutboundBatchData>>>>,
}

impl RS {
    /// Creates a new Reed-Solomon state
    pub fn new() -> Self {
        Self {
            inbound: Mutex::new(HashMap::new()),
            outbound: Mutex::new(HashMap::new()),
        }
    }

    pub async fn sent(&self, packet: FECPacket) -> Option<Vec<ParityPacket>> {
        let entry = {
            let mut guard = self.outbound.lock().await;
            guard
                .entry((packet.batch_id, packet.session_id))
                .or_insert((&packet).into())
                .clone()
        };

        let mut result = {
            let mut batch = entry.lock().await;
            batch.encoder.add_original_shard(*packet.data.0);
            batch.received_count += 1;
            if batch.received_count >= batch.batch_size {
                let recovery = batch.encoder.encode().expect("This Should not fail");
                Some(
                    recovery
                        .recovery_iter()
                        .enumerate()
                        .map(|(i, payload)| (i as u8, &packet, payload).into())
                        .collect(),
                )
            } else {
                None
            }
        };

        if result.is_some() {
            self.outbound
                .lock()
                .await
                .remove_entry(&(packet.batch_id, packet.session_id));
        }

        result
    }

    pub async fn received(&self, packet: FECPacket) -> bool {
        let entry = {
            let mut guard = self.inbound.lock().await;
            guard
                .entry((packet.batch_id, packet.session_id))
                .or_insert((&packet).into())
                .clone()
        };

        let mut batch = entry.lock().await;
        if packet.is_parity {
            batch
                .decoder
                .add_recovery_shard(packet.batch_pos as usize, *packet.data.0);
            batch.recovery_count += 1;
        } else {
            batch
                .decoder
                .add_original_shard(packet.batch_pos as usize, *packet.data.0);
            batch.received_count += 1;
        }

        batch.received_count + batch.recovery_count >= batch.batch_size
    }

    pub async fn recover(
        &self,
        batch_id: BatchID,
        session_id: SessionId,
    ) -> Option<Vec<RecoverdPacket>> {
        let Some((_, entry)) = ({
            let mut guard = self.inbound.lock().await;
            guard.remove_entry(&(batch_id, session_id))
        }) else {
            return None;
        };

        let mut batch = entry.lock().await;
        batch.decoder.decode().ok().map(|result| {
            result
                .restored_original_iter()
                .map(|data| data.1.try_into().expect("Exact size"))
                .collect()
        })
    }
}
