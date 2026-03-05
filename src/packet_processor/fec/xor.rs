use crate::packetizer::types::{
    FecInfo, OptionFlags, Options, PacketType, PacketTypeFecBatchID, ParityPacket,
};
use crate::prelude::*;

use std::collections::HashMap;
use std::mem::transmute;
use std::ops::{BitXor, BitXorAssign};
use std::sync::{LazyLock, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{
    packet_processor::{Batch, FecPacket, serialize::PacketSerialize},
    packetizer::types::{DataPacket, SessionId},
};

const FEC_DATA_SIZE: usize = ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH;

struct FECData(Box<[u64; FEC_DATA_SIZE / 8]>);

impl FECData {
    pub fn new() -> Self {
        Self(Box::new([0; FEC_DATA_SIZE / 8]))
    }
}

impl From<DataPacket> for FECData {
    fn from(value: DataPacket) -> Self {
        let mut buf = [0; FEC_DATA_SIZE];
        value.byte_range.serialize(&mut buf[..]);
        value.payload_length.serialize(&mut buf[4..]);
        value.payload.serialize(&mut buf[6..]);
        Self(Box::new(unsafe { transmute(buf) }))
    }
}

impl From<FecPacket> for FECData {
    fn from(value: FecPacket) -> Self {
        Self(Box::new(unsafe { transmute(value.data) }))
    }
}

impl Default for FECData {
    fn default() -> Self {
        Self(Box::new([0; FEC_DATA_SIZE / 8]))
    }
}

impl BitXorAssign for FECData {
    fn bitxor_assign(&mut self, rhs: Self) {
        for (d, s) in std::iter::zip(self.0.iter_mut(), rhs.0.iter()) {
            *d ^= s;
        }
    }
}

impl BitXor for FECData {
    type Output = Self;

    fn bitxor(mut self, rhs: Self) -> Self::Output {
        for (d, s) in std::iter::zip(self.0.iter_mut(), rhs.0.iter()) {
            *d ^= s;
        }
        self
    }
}

struct InboundBatchData {
    product: FECData,
    parity: FECData,
    packet_num: usize,
}

impl InboundBatchData {
    pub fn add(&mut self, data: FECData) {
        self.product ^= data;
    }

    pub fn recover(self) -> FECData {
        self.product ^ self.parity
    }
}

impl Default for InboundBatchData {
    fn default() -> Self {
        Self {
            product: Default::default(),
            parity: Default::default(),
            packet_num: 0,
        }
    }
}

struct OutboundBatchData {
    product: FECData,
    batch_size: usize,
    current_size: usize,
}

impl OutboundBatchData {
    pub fn new(batch_size: u8) -> Self {
        Self {
            product: FECData::new(),
            batch_size: batch_size as usize,
            current_size: 0,
        }
    }

    pub fn add(&mut self, data: FECData) -> bool {
        self.product ^= data;
        self.current_size += 1;
        if self.batch_size <= self.current_size {
            true
        } else {
            false
        }
    }
}

static RECEIVED_PACKETS: LazyLock<Mutex<HashMap<Batch, InboundBatchData>>> =
    LazyLock::new(Default::default);

static OUTBOUND: LazyLock<Mutex<HashMap<SessionId, OutboundBatchData>>> =
    LazyLock::new(Default::default);

pub async fn received(batch: Batch, pack: FecPacket) -> bool {
    let mut map = RECEIVED_PACKETS.lock().expect("A process panicked!");
    let pos = pack.batch_pos;
    let entry = map.entry(batch).or_default();
    entry.add(pack.into());
    pos <= entry.packet_num
}

pub async fn sent(packet: DataPacket) -> Option<ParityPacket> {
    let mut out_packets = OUTBOUND.lock().ok()?;
    let session_id = packet.session_id;
    let batch_id = packet.packet_type_batch_id.1;
    let batch_size = packet.fec_info.batch_size;
    let full = out_packets
        .entry(session_id)
        .or_insert(OutboundBatchData::new(packet.fec_info.batch_size))
        .add(packet.into());

    if full {
        Some(ParityPacket::new(
            unsafe { transmute(out_packets.remove(&session_id)?.product.0) },
            Options::construct(vec![]),
            PacketTypeFecBatchID(PacketType::Parity, batch_id),
            FecInfo {
                batch_size,
                batch_pos: 0,
            },
            session_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time should move forward")
                .as_millis() as u64,
        ))
    } else {
        None
    }
}
