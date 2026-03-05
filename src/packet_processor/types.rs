use crate::{packetizer::types::ParityPacket, prelude::*};

pub use crate::{packetizer::types::PacketWrapper, transport::types::ReceivedPacket};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::packetizer::types::{MAX_PAYLOAD_LENGTH, Options, PacketType, SessionId};

pub struct InboundChannels {
    pub t_receiver: Receiver<Result<ReceivedPacket>>,
    pub p_sender: Sender<Result<PacketWrapper>>,
}

pub struct OutboundChannels {
    pub t_sender: Sender<TransportMessage>,
    pub p_sender: Sender<Result<PacketWrapper>>,
    pub p_receiver: Receiver<PacketProcessingMessage>,
}

struct PacketIdentifiers {
    pub session_id: SessionId,
    pub packet_type: PacketType,
    pub opts: Options,
    pub timestamp_ms: u64,
}

#[repr(C)]
#[derive(Hash, PartialEq, Eq)]
pub struct Batch {
    pub batch_id: u16,
    pub batch_size: u8,
}

#[derive(Hash, Eq)]
pub struct FecPacket {
    pub is_data: bool,
    pub batch_pos: u8,
    pub data: [u8; ParityPacket::LOCAL_MAX_PAYLOAD_LENGTH],
}

impl PartialEq for FecPacket {
    fn eq(&self, other: &Self) -> bool {
        self.batch_pos == other.batch_pos && self.is_data == other.is_data
    }
}

/// Messages sent to the packet processing layer from the packetizer.
/// Used to send packets for processing or signal graceful shutdown.
pub enum PacketProcessingMessage {
    SendPacket(PacketWrapper),
    Close,
}

/// Messages sent to the transport send task.
/// Contains either processed packets ready for transmission or a close signal.
/// Upon receiving Close, the task will wait to confirm all packets were sent.
#[derive(Debug, Clone)]
pub enum TransportMessage {
    Data(Vec<ProcessedPacket>),
    Close,
}

/// Unique identifier for a packet, used primarily for tracking and resending.
/// The timestamp is extracted from packet headers as they are produced from the packetizer layer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PacketId {
    pub timestamp: u64,
    pub session_id: SessionId,
}

/// Represents a serialized packet with minimal data necessary for the transport layer.
/// Contains the encrypted packet data along with metadata needed for transmission
/// and retransmission logic. Uses Vec<u8> since it can represent any packet type.
#[derive(Clone, Debug)]
pub struct ProcessedPacket {
    pub packet_id: PacketId,
    pub packet_type: PacketType,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}
