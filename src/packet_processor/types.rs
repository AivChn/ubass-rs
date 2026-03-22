use std::net::SocketAddrV4;

use crate::prelude::*;

pub use crate::{packetizer::types::PacketWrapper, transport::types::ReceivedPacket};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::packetizer::types::{PacketType, SessionId};

/// packages the channels needed for the inbound task
pub struct InboundChannels {
    pub t_receiver: Receiver<Result<ReceivedPacket>>,
    pub p_sender: Sender<Result<PacketWrapper>>,
}

/// packages the channels needed for the outbound task
pub struct OutboundChannels {
    pub t_sender: Sender<TransportMessage>,
    pub p_sender: Sender<Result<PacketWrapper>>,
    pub p_receiver: Receiver<PacketProcessingMessage>,
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
    pub dest_addr: SocketAddrV4,
    pub packet_id: PacketId,
    pub packet_type: PacketType,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}
