#![allow(unused)]

pub mod inference;
mod reed_solomon;
mod xor;

use std::mem::transmute;
use std::sync::LazyLock;

use crate::packet_processor::serialize::Serialize;

use crate::manager::packets::{
    BatchID, BytePosition, DataPacket, FECInfo, FecScheme, MAX_PAYLOAD_LENGTH,
};
use crate::manager::packets::{ParityPacket, SessionId};
use crate::transport::types::ReceivedPacket;

use derive_more::Deref;
use tokio::sync::OnceCell;

/// alias for the max size of the payload
const FEC_DATA_SIZE: usize = ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH;

/// Wrapper for the data this module will work on
#[derive(Deref, Debug, Clone)]
#[repr(align(32))]
struct FECData(Box<[u8; FEC_DATA_SIZE]>);

impl From<&DataPacket> for FECData {
    #[allow(clippy::cast_possible_truncation)]
    fn from(value: &DataPacket) -> Self {
        let mut buf = Box::new([0; FEC_DATA_SIZE]);
        value.byte_range_start.serialize(&mut buf[..]);
        debug_assert!(value.payload.serialize(&mut buf[4..]).is_ok());
        // payload length goes in the last 2 bytes of FECData, where
        // `From<FECData> for RecoverdPacket` reads it back after XOR recovery.
        (value.payload.len() as u16).serialize(&mut buf[FEC_DATA_SIZE - 2..]);
        Self(buf)
    }
}

impl From<FECPacket> for FECData {
    fn from(value: FECPacket) -> Self {
        Self(value.data.0)
    }
}

impl Default for FECData {
    fn default() -> Self {
        Self(Box::new([0; FEC_DATA_SIZE]))
    }
}

/// Represents a recovered packet, includes all necessary data for full recovery
#[derive(Debug)]
pub struct RecoverdPacket {
    pub byte_range_start: BytePosition,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct NotARecoveredPacket;

impl TryFrom<&[u8]> for RecoverdPacket {
    type Error = NotARecoveredPacket;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < FEC_DATA_SIZE {
            Err(NotARecoveredPacket)
        } else {
            Ok(RecoverdPacket {
                byte_range_start: BytePosition(
                    <u32>::deserialize(&value[..4]).expect("Exact size"),
                ),
                payload: Vec::from(
                    <[u8; MAX_PAYLOAD_LENGTH]>::try_from(&value[4..4 + MAX_PAYLOAD_LENGTH])
                        .expect("Exact size"),
                ),
            })
        }
    }
}

impl From<FECData> for RecoverdPacket {
    fn from(value: FECData) -> Self {
        let byte_range_start =
            BytePosition(<u32>::deserialize(&value.0[..4]).expect("Size is exact"));
        let mut payload: Vec<u8> = Vec::from(&value.0[4..value.0.len() - 2]);
        debug_assert_eq!(payload.len(), MAX_PAYLOAD_LENGTH);

        let payload_len = u16::deserialize(&value.0[value.0.len() - 2..]).expect("exact size");
        payload.truncate(payload_len as usize);

        RecoverdPacket {
            byte_range_start,
            payload,
        }
    }
}

#[derive(Debug)]
pub struct Recovered {
    pub session_id: SessionId,
    pub batch_id: BatchID,
    pub packets: Vec<RecoverdPacket>,
}

/// Represents a packet to be processed. Includes only the data necessary.
///
/// `is_parity`: `true` if the packet is a parity packet
/// `session_id`: the session id for the session this packet is sent through
/// `batch_id`: the ID for the specific batch within a session
/// `batch_size`: the number of packets expected in this batch
struct FECPacket {
    is_parity: bool,
    session_id: SessionId,
    batch_id: BatchID, // u10 in practice
    fec_info: FECInfo,
    data: FECData, // 1404 bytes
}

impl From<DataPacket> for FECPacket {
    fn from(value: DataPacket) -> Self {
        let data = FECData::from(&value);
        FECPacket {
            is_parity: false,
            session_id: value.session_id,
            batch_id: value.batch_id,
            fec_info: value.fec_info,
            data,
        }
    }
}

impl From<ParityPacket> for FECPacket {
    fn from(value: ParityPacket) -> Self {
        let mut data = FECData::default();
        value.payload.serialize(&mut data.0[..FEC_DATA_SIZE]);
        FECPacket {
            is_parity: true,
            session_id: value.session_id,
            batch_id: value.batch_id,
            fec_info: value.fec_info,
            data,
        }
    }
}

#[allow(private_bounds)]
pub trait FECCompatible: Into<FECPacket> {}
impl FECCompatible for DataPacket {}
impl FECCompatible for ParityPacket {}

static XOR: LazyLock<xor::Xor> = LazyLock::new(xor::Xor::new);
static RS: LazyLock<reed_solomon::RS> = LazyLock::new(reed_solomon::RS::new);

/// Dispatch a sent data packet to the FEC codec selected by the packet's own
/// `fec_info.scheme`. Returns the parity packets if the batch is now full.
pub async fn sent(packet: impl FECCompatible) -> Option<Vec<ParityPacket>> {
    let fec_packet: FECPacket = packet.into();
    match fec_packet.fec_info.scheme {
        FecScheme::Xor => XOR.sent(fec_packet).await,
        FecScheme::ReedSolomon => RS.sent(fec_packet).await,
    }
}

/// Dispatch an inbound data/parity packet to the FEC codec selected by the
/// packet's own `fec_info.scheme`. Returns `true` if the batch is recoverable.
pub async fn received(packet: impl FECCompatible) -> bool {
    let fec_packet: FECPacket = packet.into();
    match fec_packet.fec_info.scheme {
        FecScheme::Xor => XOR.received(fec_packet).await,
        FecScheme::ReedSolomon => RS.received(fec_packet).await,
    }
}

/// Recover a batch's missing packets. The caller must remember the scheme it
/// saw on the triggering packet (via `packet.fec_info.scheme`) since the
/// packet has been moved into the codec by the time `recover` runs.
pub async fn recover(
    batch_id: BatchID,
    session_id: SessionId,
    scheme: FecScheme,
) -> Option<Recovered> {
    let packets = match scheme {
        FecScheme::Xor => XOR.recover(batch_id, session_id).await,
        FecScheme::ReedSolomon => RS.recover(batch_id, session_id).await,
    }?;
    Some(Recovered {
        session_id,
        batch_id,
        packets,
    })
}

/// Sweep both codecs. Each owns its own batch tables; expired entries are
/// returned together so the caller can drive retransmits uniformly.
pub async fn prune(ttl_ms: u64) -> Vec<(SessionId, BatchID, Vec<BytePosition>)> {
    let mut out = XOR.prune(ttl_ms).await;
    out.extend(RS.prune(ttl_ms).await);
    out
}
