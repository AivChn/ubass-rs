use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;
use tokio::time::Instant;

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use crate::{
    packet_processor::fec::FECPacket,
    packetizer::types::{BatchID, FECInfo, Options, PacketType, ParityPacket, SessionId, Version},
    prelude::*,
};

use super::{FEC_DATA_SIZE, RecoverdPacket};

struct InboundBatchData {
    decoder: ReedSolomonDecoder,
    batch_size: u8,
    recovery_count: u8,
    received_count: u8,
}

impl From<&FECPacket> for Arc<Mutex<InboundBatchData>> {
    fn from(value: &FECPacket) -> Self {
        let batch_size = value.fec_info.batch_size;
        let recovery_count = value.fec_info.recovery_count;
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
        let batch_size = value.fec_info.batch_size;
        let recovery_count = value.fec_info.recovery_count;
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
            Options::construct(&[]),
            packet.batch_id,
            FECInfo::new(
                packet.fec_info.batch_size,
                i,
                packet.fec_info.recovery_count,
            ),
            packet.session_id,
            payload.into(),
        )
    }
}

type MapKey = (BatchID, SessionId);
type InboundBatch = Arc<Mutex<InboundBatchData>>;
type OutboundBatch = Arc<Mutex<OutboundBatchData>>;

/// Represents the full state of the Reed-Solomon FEC strategy
pub struct RS {
    inbound: Mutex<HashMap<MapKey, InboundBatch>>,
    outbound: Mutex<HashMap<MapKey, OutboundBatch>>,
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
                    #[allow(clippy::cast_possible_truncation)]
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
            if let Err(reed_solomon_simd::Error::DuplicateOriginalShardIndex { index: _ }) = batch
                .decoder
                .add_recovery_shard(packet.fec_info.batch_pos as usize, *packet.data.0)
            {
                return false;
            }
            batch.recovery_count += 1;
        } else {
            if let Err(reed_solomon_simd::Error::DuplicateOriginalShardIndex { index: _ }) = batch
                .decoder
                .add_original_shard(packet.fec_info.batch_pos as usize, *packet.data.0)
            {
                return false;
            }
            batch.received_count += 1;
        }

        true
    }

    pub async fn recover(
        &self,
        batch_id: BatchID,
        session_id: SessionId,
    ) -> Option<Vec<RecoverdPacket>> {
        let (_, entry) = ({
            let mut guard = self.inbound.lock().await;
            guard.remove_entry(&(batch_id, session_id))
        })?;

        let mut batch = entry.lock().await;
        batch.decoder.decode().ok().map(|result| {
            result
                .restored_original_iter()
                .map(|data| data.1.try_into().expect("Exact size"))
                .collect()
        })
    }
}
