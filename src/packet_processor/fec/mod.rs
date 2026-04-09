#![allow(unused)]

mod reed_solomon;
mod xor;

use std::mem::transmute;
use std::sync::LazyLock;

use crate::packet_processor::serialize::Serialize;

use crate::manager::packets::{BatchID, DataPacket, FECInfo, MAX_PAYLOAD_LENGTH};
use crate::manager::packets::{ParityPacket, SessionId};
use crate::transport::types::ReceivedPacket;

use tokio::sync::OnceCell;

/// alias for the max size of the payload
const FEC_DATA_SIZE: usize = ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH;

/// Wrapper for the data this module will work on
#[derive(Debug, Clone)]
#[repr(align(32))]
struct FECData(Box<[u8; FEC_DATA_SIZE]>);

impl From<DataPacket> for FECData {
    fn from(value: DataPacket) -> Self {
        let mut buf = Box::new([0; FEC_DATA_SIZE]);
        value.byte_range_start.serialize(&mut buf[..]);
        value.payload.serialize(&mut buf[4..]);
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
pub struct RecoverdPacket {
    byte_range_start: u32,
    payload: Box<[u8; MAX_PAYLOAD_LENGTH]>,
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
                byte_range_start: <u32>::deserialize(&value[..4]).expect("Exact size"),
                payload: Box::new(
                    <[u8; MAX_PAYLOAD_LENGTH]>::try_from(&value[4..4 + MAX_PAYLOAD_LENGTH])
                        .expect("Exact size"),
                ),
            })
        }
    }
}

impl From<FECData> for RecoverdPacket {
    fn from(value: FECData) -> Self {
        let byte_range_start = <u32>::deserialize(&value.0[..4]).expect("Size is exact");
        let payload: Box<[u8; MAX_PAYLOAD_LENGTH]> = Box::new(
            value.0[4..]
                .try_into()
                .expect("size is expected to be exactly matching"),
        );

        RecoverdPacket {
            byte_range_start,
            payload,
        }
    }
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
        let mut data = FECData::default();
        value.byte_range_start.serialize(&mut data.0[..]);
        value.payload.serialize(&mut data.0[4..]);
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

#[cfg(feature = "fec_xor")]
type FECImpl = xor::Xor;

#[cfg(not(feature = "fec_xor"))]
type FECImpl = reed_solomon::RS;

static FEC: LazyLock<FECImpl> = LazyLock::new(FECImpl::new);

pub async fn sent(packet: impl FECCompatible) -> Option<Vec<ParityPacket>> {
    FEC.sent(packet.into()).await
}

pub async fn received(packet: impl FECCompatible) -> bool {
    let packet: FECPacket = packet.into();
    FEC.received(packet).await
}

pub async fn recover(batch_id: BatchID, session_id: SessionId) -> Option<Vec<RecoverdPacket>> {
    FEC.recover(batch_id, session_id).await
}
