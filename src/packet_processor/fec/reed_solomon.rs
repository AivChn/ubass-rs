use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use reed_solomon_simd::{ReedSolomonDecoder, ReedSolomonEncoder};

use crate::{
    manager::packets::{
        BatchID, BytePosition, FECInfo, FecScheme, MAX_PAYLOAD_LENGTH, Options, PacketType,
        ParityPacket, SessionId, Version,
    },
    packet_processor::fec::FECPacket,
    packet_processor::serialize::Serialize,
    prelude::*,
};

use super::{FEC_DATA_SIZE, RecoverdPacket};

struct InboundBatchData {
    decoder: ReedSolomonDecoder,
    batch_size: u8,
    recovery_count: u8,
    received_count: u8,
    // Bitmask of received data batch_pos values (analogous to xor.rs). Used to
    // enumerate missing positions when the batch is pruned.
    batch_mask: [u128; 2],
    created_at: Timestamp,
    base_byte_pos: Option<BytePosition>,
    is_contiguous: bool,
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
            batch_mask: [0; 2],
            created_at: Timestamp::now(),
            base_byte_pos: None,
            is_contiguous: true,
        }))
    }
}

impl InboundBatchData {
    #[allow(clippy::cast_possible_truncation)]
    fn missing_positions(&self) -> Vec<BytePosition> {
        let Some(base) = self.base_byte_pos else {
            return vec![];
        };
        if !self.is_contiguous {
            return vec![];
        }
        (0..self.batch_size)
            .filter(|i| (self.batch_mask[(i / 128) as usize] >> (i % 128)) & 1 == 0)
            .map(|i| BytePosition(base.0 + u32::from(i) * MAX_PAYLOAD_LENGTH as u32))
            .collect()
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
                FecScheme::ReedSolomon,
            ),
            packet.session_id,
            payload,
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
            // Capture base + contiguity from byte_range_start (FECData[0..4])
            // before handing the shard to the decoder. Mirrors xor.rs.
            let pkt_byte_pos =
                BytePosition(<u32>::deserialize(&packet.data.0[..4]).expect("Exact size"));
            #[allow(clippy::cast_possible_truncation)]
            let derived_base = BytePosition(
                pkt_byte_pos.0.saturating_sub(
                    u32::from(packet.fec_info.batch_pos) * MAX_PAYLOAD_LENGTH as u32,
                ),
            );
            match batch.base_byte_pos {
                None => batch.base_byte_pos = Some(derived_base),
                Some(existing) if existing.0 != derived_base.0 => {
                    batch.is_contiguous = false;
                }
                _ => {}
            }
            batch.batch_mask[(packet.fec_info.batch_pos / 128) as usize] |=
                1 << (packet.fec_info.batch_pos % 128);

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

    /// Sweep inbound batches whose age exceeds `ttl_ms`. Removes them from
    /// the map and returns `(session_id, batch_id, missing_positions)` for each.
    /// Outer `inbound` mutex held only briefly twice — see xor.rs for rationale.
    pub async fn prune(&self, ttl_ms: u64) -> Vec<(SessionId, BatchID, Vec<BytePosition>)> {
        let snapshot: Vec<(MapKey, InboundBatch)> = {
            let guard = self.inbound.lock().await;
            guard.iter().map(|(k, v)| (*k, v.clone())).collect()
        };

        let mut expired_keys = Vec::new();
        for (key, entry) in &snapshot {
            let batch = entry.lock().await;
            if batch.created_at.been_longer_than(ttl_ms) {
                expired_keys.push(*key);
            }
        }

        let removed: Vec<(MapKey, InboundBatch)> = {
            let mut guard = self.inbound.lock().await;
            expired_keys
                .into_iter()
                .filter_map(|k| guard.remove(&k).map(|e| (k, e)))
                .collect()
        };

        let mut out = Vec::with_capacity(removed.len());
        for (key, entry) in removed {
            let batch = entry.lock().await;
            out.push((key.1, key.0, batch.missing_positions()));
        }
        out
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
