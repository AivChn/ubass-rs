use crate::{
    packet_processor::{Batch, FecPacket},
    packetizer::types::{DataPacket, PacketType, ParityPacket, SessionId},
};
use reed_solomon_simd;
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

struct BatchFull;
impl From<DataPacket> for FecPacket {
    fn from(value: DataPacket) -> Self {
        let mut data: Vec<u8> = vec![];
        data.extend(value.byte_range.start.to_be_bytes());
        data.extend(value.byte_range.length.to_be_bytes());
        data.extend(value.payload_length.to_be_bytes());
        data.extend(value.payload.iter());
        Self {
            is_data: value.packet_type_batch_id.0 == PacketType::Data,
            batch_pos: value.fec_info.batch_pos,
            data: *value.payload,
        }
    }
}

impl AsRef<[u8]> for FecPacket {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

static RECEIVED_PACKETS: LazyLock<Mutex<HashMap<Batch, Option<HashSet<FecPacket>>>>> =
    LazyLock::new(Default::default);

static OUTBOUND: LazyLock<Mutex<HashMap<SessionId, OutboundBatch>>> =
    LazyLock::new(Default::default);

pub async fn received(batch: Batch, pack: FecPacket) -> bool {
    // get received table
    let mut table = RECEIVED_PACKETS.lock().await;
    let batch_size = batch.batch_size as usize;
    // find batch or create
    let entry = table.entry(batch).or_insert(Some(HashSet::new()));

    if let Some(entry) = entry {
        if entry.len() <= batch_size {
            entry.insert(pack);
        } else {
            return false;
        }
    }

    true
}

// adds the given packet to the batch, if this causes the batch to fill parity will be derived
// and returned, otherwise None will be returned
pub async fn sent(packet: DataPacket) -> Option<ParityPacket> {
    let mut table = OUTBOUND.lock().await;
    let entry = table
        .entry(packet.session_id)
        .or_insert(OutboundBatch::new());
    entry.packets.push(FecPacket::from(packet));

    if entry.packets.len() >= entry.batch_size as usize {
        // TODO: implement derive_parity call
        todo!()
    }

    None
}

fn derive_parity(mut entry: OutboundBatch) -> Option<FecPacket> {
    entry
        .packets
        .sort_by(|p1, p2| p1.batch_pos.cmp(&p2.batch_pos));
    let Ok(value) = reed_solomon_simd::encode(
        entry.batch_size as usize,
        (entry.batch_size / 5) as usize,
        entry.packets,
    ) else {
        return None;
    };

    //TODO: Parity packet will be changed - wait with this until thats done
    let parity_packets: Vec<FecPacket> = value.iter().map(|p| ParityPacket::new(p.try_into()));

    None
}
