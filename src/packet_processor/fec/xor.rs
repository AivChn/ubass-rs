use crate::packet_processor::{inbound, outbound};
use crate::{lock, prelude::*};

use std::collections::HashMap;
use std::mem::transmute;
use std::ops::{BitXor, BitXorAssign};
use std::sync::{Arc, LazyLock};
use std::vec;

use tokio::sync::Mutex;

use crate::{
    manager::packets::{
        BatchID, BytePosition, DataPacket, FECInfo, MAX_PAYLOAD_LENGTH, Options, PacketType,
        SessionId,
    },
    packet_processor::serialize::Serialize,
};

use super::{FECData, FECPacket, ParityPacket, RecoverdPacket};

/// impl `^=` for `FECData`
impl BitXorAssign for FECData {
    fn bitxor_assign(&mut self, rhs: Self) {
        self.0
            .iter_mut()
            .zip(rhs.0.iter())
            .for_each(|(a, b)| *a ^= b);
    }
}

impl BitXor for FECData {
    type Output = FECData;

    /// impl `^` for `FECData`
    fn bitxor(mut self, rhs: Self) -> Self::Output {
        self ^= rhs;
        self
    }
}

#[allow(clippy::doc_markdown)]
/// Represents a sinle inbound batch
///
/// `product`: The result of "XORing" all received packets other than parity.
/// `parity`: The parity packet, if one was received.
/// `batch_size`: const, the number of packets expected this batch.
/// `packets_received`: number of packets received.
#[derive(Debug, Clone)]
struct InboundBatchData {
    product: FECData,
    parity: Option<FECData>,
    batch_size: u8,
    data_received: u8,
    batch_mask: [u128; 2],
    created_at: Timestamp,
    // Set on first received data packet from `byte_range_start - batch_pos * MPL`.
    // Used to enumerate missing positions for pruned batches; only meaningful
    // when `is_contiguous` is true.
    base_byte_pos: Option<BytePosition>,
    // True when every received packet's byte position matches
    // `base_byte_pos + batch_pos * MPL`. Retransmit batches break this; for
    // those we can't enumerate the missing positions on prune.
    is_contiguous: bool,
}

impl InboundBatchData {
    /// Creates a new inbound batch with defaults.
    /// takes a `batch_size` u8
    fn new(batch_size: u8) -> Self {
        Self {
            product: FECData::default(),
            parity: None,
            batch_size,
            data_received: 0,
            batch_mask: [0; 2],
            created_at: Timestamp::now(),
            base_byte_pos: None,
            is_contiguous: true,
        }
    }

    /// Add a packet to the FEC batch. Returns `true` if the packet wasnt already in the batch
    fn add(&mut self, data: FECPacket) -> bool {
        if self.data_received < self.batch_size
            && (self.batch_mask[(data.fec_info.batch_pos / 128) as usize]
                >> (data.fec_info.batch_pos % 128))
                & 1
                == 0
        {
            // Capture base byte position from this packet's byte_range_start
            // (lives at FECData[0..4]) before XORing it into product. Track
            // contiguity so prune knows whether to bother enumerating.
            let pkt_byte_pos =
                BytePosition(<u32>::deserialize(&data.data.0[..4]).expect("Exact size"));
            #[allow(clippy::cast_possible_truncation)]
            let derived_base =
                BytePosition(pkt_byte_pos.0.saturating_sub(
                    u32::from(data.fec_info.batch_pos) * MAX_PAYLOAD_LENGTH as u32,
                ));
            match self.base_byte_pos {
                None => self.base_byte_pos = Some(derived_base),
                Some(existing) if existing.0 != derived_base.0 => {
                    self.is_contiguous = false;
                }
                _ => {}
            }

            self.batch_mask[(data.fec_info.batch_pos / 128) as usize] |=
                1 << (data.fec_info.batch_pos % 128);
            self.product ^= data.data;
            self.data_received += 1;
            true
        } else {
            false
        }
    }

    /// Enumerate the byte positions of chunks the batch expected but never received.
    /// Returns an empty `Vec` for non-contiguous batches (retransmit batches), where
    /// the receiver can't infer expected positions on its own.
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

    /// To be used when the parity packet is received, returns `true` if the packet wasnt already
    /// in the batch
    fn parity(&mut self, parity: FECData) -> bool {
        if self.parity.is_some() {
            false
        } else {
            self.parity = Some(parity);
            true
        }
    }

    /// Recover a packet from this batch. This function returns `Some(ParityPacket)` if there are
    /// enough packets in the batch and the parity packet exists. Otherwise returns `None`
    fn recover(self) -> Option<FECData> {
        if self.data_received == self.batch_size - 1
            && let Some(parity) = self.parity
        {
            Some(self.product ^ parity)
        } else {
            None
        }
    }
}

impl From<InboundBatchData> for Arc<Mutex<InboundBatchData>> {
    #[inline]
    fn from(value: InboundBatchData) -> Self {
        Arc::new(Mutex::new(value))
    }
}

#[allow(clippy::doc_markdown)]
/// Represents an outbound batch
///
/// `product`: the result of "XORing" all the received packets so far.
/// `batch_size`: const, the number of packets expected this batch.
/// `current_size`: how many packets are currently in the batch.
struct OutboundBatchData {
    product: FECData,
    batch_size: usize,
    current_size: usize,
}

impl OutboundBatchData {
    /// Creates a new outbound batch.
    /// Takes a `batch_size` u8
    fn new(batch_size: u8) -> Self {
        Self {
            product: FECData::default(),
            batch_size: batch_size as usize,
            current_size: 0,
        }
    }

    /// Adds another packet to the prodcut.
    /// If this packet completes the batch (`current_size` hits `batch_size` for the
    /// first time), returns `true`; on subsequent over-fills returns `false` so
    /// only a single caller proceeds to remove the batch from the outbound map.
    fn add(&mut self, data: FECData) -> bool {
        self.product ^= data;
        self.current_size += 1;
        self.batch_size == self.current_size
    }
}

impl From<OutboundBatchData> for Arc<Mutex<OutboundBatchData>> {
    #[inline]
    fn from(value: OutboundBatchData) -> Self {
        Arc::new(Mutex::new(value))
    }
}

type MapKey = (BatchID, SessionId);
type InboundBatch = Arc<Mutex<InboundBatchData>>;
type OutboundBatch = Arc<Mutex<OutboundBatchData>>;

/// Represents the full state of the FEC module.
/// `inbound`: a hashmap of `(batch_id, session_id)` to a batch of inbound packets.
/// `outbound`: a hashmap of `(batch_id, session_id)` to a batch of outbound packets.
///
/// The values are behind `Arc<Mutex<>>` to allow for asynchronous tasks to access the value without
/// locking the whole hashmap for the full duration of the operation.
pub struct Xor {
    inbound: Mutex<HashMap<MapKey, InboundBatch>>,
    outbound: Mutex<HashMap<MapKey, OutboundBatch>>,
}

impl Xor {
    /// creates a new empty XOR struct
    pub fn new() -> Self {
        Self {
            inbound: Mutex::new(HashMap::new()),
            outbound: Mutex::new(HashMap::new()),
        }
    }

    /// Used with a sent packet. returns `Some(ParityPacket)` if the batch was filled because of
    /// this operation, otherwise returns `None`
    /// `Vec` is used just to allow generalization with Reed-Solomon implementation
    ///
    /// **CAN PACIC** if the batch is somehow removed before all packets are added
    pub async fn sent(&self, packet: FECPacket) -> Option<Vec<ParityPacket>> {
        // get entry
        let mut entry = {
            let mut guard = self.outbound.lock().await;
            guard
                .entry((packet.batch_id, packet.session_id))
                .or_insert(OutboundBatchData::new(packet.fec_info.batch_size).into())
                .clone()
        };

        let batch_ready = {
            // if the batch was *not* filled after adding this packet:
            let mut batch = entry.lock().await;
            batch.add(FECData(packet.data.0)) // <- returns `bool`
        };

        if batch_ready {
            // if it was:
            let (_, value) = {
                let mut guard = self.outbound.lock().await;
                guard
                    .remove_entry(&(packet.batch_id, packet.session_id))
                    .expect("invariant borken: batch does not exist")
            };

            let batch = value.lock().await;

            // prepare parity packet fields
            let payload = batch.product.0.clone();
            let opts = Options::construct(&[]);
            let fec_info = FECInfo::new(packet.fec_info.batch_size, 0, 1);
            let session_id = packet.session_id;

            Some(vec![ParityPacket::new(
                opts,
                packet.batch_id,
                fec_info,
                session_id,
                payload.to_vec(),
            )])
        } else {
            None
        }
    }

    /// Handled received packets (inbound)
    /// returns true if packets can be recovered.
    pub async fn received(&self, packet: FECPacket) -> bool {
        let entry = {
            let mut guard = self.inbound.lock().await;
            guard
                .entry((packet.batch_id, packet.session_id))
                .or_insert(InboundBatchData::new(packet.fec_info.batch_size).into())
                .clone()
        };

        let mut entry = lock!(entry);
        if packet.is_parity {
            entry.parity(packet.data);
        } else {
            entry.add(packet);
        }

        (entry.data_received == entry.batch_size - 1) && entry.parity.is_some()
    }

    /// Handles recovering a packet from an inbound batch.
    /// returns `Some(RecoverdPacket)` if the batch exists.
    /// The return value is a `Vec` for parity with the Reed-Solomon implementation.
    ///
    /// **CAN PANIC** if the batch isn't ready yet. Handle this invariant up in the chain.
    pub async fn recover(
        &self,
        batch_id: BatchID,
        session_id: SessionId,
    ) -> Option<Vec<RecoverdPacket>> {
        // if there is no entry return None
        let entry = ({
            let mut guard = self.inbound.lock().await;
            guard.remove(&(batch_id, session_id))
        })?;

        // HACK: This shouldnt clone in the final implementation
        lock!(entry).clone().recover().map(|e| vec![e.into()])
    }

    /// Sweep inbound batches whose age exceeds `ttl_ms`. Removes them from
    /// the map and returns `(session_id, batch_id, missing_positions)` for
    /// each. Recoverable / full batches exit via `recover`, never here.
    ///
    /// The outer `inbound` mutex is held only briefly twice — once to
    /// snapshot the live entries and once to remove the expired ones — so
    /// concurrent `received` / `recover` calls don't queue behind us.
    /// Per-batch timestamp checks and `missing_positions` computation run
    /// against the snapshot (Arc clones), outside the outer mutex.
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
}
