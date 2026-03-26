use crate::packet_processor::{inbound, outbound};
use crate::packetizer::types::{BatchID, FECInfo, Options, PacketType, PacketTypeFecBatchID};
use crate::prelude::*;

use std::collections::HashMap;
use std::mem::transmute;
use std::ops::{BitXor, BitXorAssign};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use futures::future::Map;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::{
    packet_processor::serialize::Serialize,
    packetizer::types::{DataPacket, SessionId},
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

/// impl `^` for `FECData`
impl BitXor for FECData {
    type Output = FECData;

    fn bitxor(mut self, rhs: Self) -> Self::Output {
        self ^= rhs;
        self
    }
}
#[allow(clippy::doc_markdown)]
/// Represents a sinle inbound batch
///
/// `product`: The result of XORing all received packets other than parity.
/// `parity`: The parity packet, if one was received.
/// `batch_size`: const, the number of packets expected this batch.
/// `packets_received`: number of packets received.
struct InboundBatchData {
    product: FECData,
    parity: Option<FECData>,
    batch_size: u8,
    packets_received: u8,
    batch_mask: [u128; 2],
}

impl InboundBatchData {
    /// Creates a new inbound batch with defaults.
    /// takes a `batch_size` u8
    fn new(batch_size: u8) -> Self {
        Self {
            product: FECData::default(),
            parity: None,
            batch_size,
            packets_received: 0,
            batch_mask: [0; 2],
        }
    }

    /// Add a packet to the FEC batch. Returns `true` if the packet wasnt already in the batch
    fn add(&mut self, data: FECPacket) -> bool {
        if self.packets_received < self.batch_size
            && (self.batch_mask[(data.fec_info.batch_pos / 128) as usize]
                >> data.fec_info.batch_pos % 128)
                & 1
                == 0
        {
            self.product ^= data.data;
            self.packets_received += 1;
            true
        } else {
            false
        }
    }

    /// To be used when the parity packet is received, returns `true` if the packet wasnt already
    /// in the batch
    fn parity(&mut self, parity: FECData) -> bool {
        if self.parity.is_some() {
            false
        } else {
            self.parity = Some(parity);
            self.packets_received += 1;
            true
        }
    }

    /// Recover a packet from this batch. This function returns `Some(ParityPacket)` if there are
    /// enough packets in the batch and the parity packet exists. Otherwise returns `None`
    fn recover(self) -> Option<FECData> {
        if self.packets_received >= self.batch_size
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
/// `product`: the result of XORing all the received packets so far.
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
    /// If the batch is full, returns `true`
    fn add(&mut self, data: FECData) -> bool {
        self.product ^= data;
        self.current_size += 1;
        self.batch_size <= self.current_size
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
    /// returns the number of packets that can be recovered. The return value is a `u8` for parity with the Reed-Solomon implementation.
    pub async fn received(&self, packet: FECPacket) -> bool {
        let entry = {
            let mut guard = self.inbound.lock().await;
            guard
                .entry((packet.batch_id, packet.session_id))
                .or_insert(InboundBatchData::new(packet.fec_info.batch_size).into())
                .clone()
        };

        if packet.is_parity {
            entry.lock().await.parity(FECData(packet.data.0))
        } else {
            entry.lock().await.add(packet)
        }
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

        // get the parity, assuming it exists
        let parity = {
            let mut batch = entry.lock().await;
            batch.parity.clone()
        }
        .expect("This function should not be called before parity is ready");

        Some(vec![parity.into()])
    }
}
