use aes_gcm_siv::aead::Payload;
use bincode;
use core::time;
use serde::{Deserialize, Serialize};
use std::{time::Instant, vec};

const MAX_PAYLOAD_LENGTH: usize = 1400;
const CURRENT_VERSION: usize = (0 << 12) | (0 << 8) | 1;

/// Enum of all possible packet types as of now
#[derive(Clone, Debug)]
pub enum PacketType {
    Data,
    Metadata,
    Parity,
    Ack,
    Control,
    ConnectionStat,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DataPacket {
    version: u16,
    packet_type: u8,
    reserved: u8,
    opts: u16,
    session_id: u64,
    timestamp_ms: u64,
    byte_range_start: u32,
    byte_range_offset: u16,
    payload_length: u16,
    payload: Vec<u8>,
}

impl DataPacket {
    fn new(
        opts: u16,
        session_id: u64,
        byte_range_start: u32,
        byte_range_offset: u16,
        payload_length: u16,
        payload: &[u8],
    ) -> Option<Self> {
        Some(Self {
            version: CURRENT_VERSION,
            packet_type: PacketType::Data as u8,
            reserved: 0,
            opts,
            session_id,
            timestamp_ms: Instant::now().into(),
            byte_range_start,
            byte_range_offset,
            payload_length,
            payload: if payload_length <= MAX_PAYLOAD_LENGTH {
                Vec::from(payload[..payload_length])
            } else {
                return None;
            },
        })
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct AckPacket {
    version: u16,
    packet_type: u8,
    reserved: u8,
    opts: u16,
    session_id: u64,
    timestamp_ms: u64,
    ack_timestamp_ms: u64,
}

impl AckPacket {
    fn new(opts: u16, session_id: u64, ack_timestamp_ms: u64) -> Self {
        Self {
            version: CURRENT_VERSION,
            packet_type: PacketType::Ack as u8,
            reserved: 0,
            opts,
            session_id,
            timestamp_ms: Instant::now().into(),
            ack_timestamp_ms,
        }
    }
}

enum ControlType {
    Retransmit,
    Play,
    Stop,
    Restart,
    Pause,
    Seek,
    SendMetadata,
    NewEncryptionKey,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ControlPacket {
    version: u16,
    packet_type: u8,
    control_type: ControlType,
    opts: u16,
    session_id: u64,
    timestamp_ms: u32,
    payload_length: u16,
    payload: Vec<u8>,
}

impl ControlPacket {
    fn new(
        control_type: ControlType,
        opts: u16,
        session_id: u64,
        payload_length: u16,
        payload: &[u8],
    ) -> Option<Self> {
        Some(Self {
            version: CURRENT_VERSION,
            packet_type: PacketType::Control as u8,
            control_type,
            opts,
            session_id,
            timestamp_ms: Instant::now().into(),
            payload_length,
            payload: if payload_length <= MAX_PAYLOAD_LENGTH {
                Vec::from(payload[..payload_length])
            } else {
                return None;
            },
        })
    }
}
