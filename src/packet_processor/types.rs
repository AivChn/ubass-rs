use std::net::{SocketAddr, SocketAddrV4};

use crate::prelude::*;

use tokio::sync::mpsc::{Receiver, Sender};

use crate::manager::packets::PacketType;

// TYPES

pub type InboundReceiver = Receiver<Result<PacketProcessingMessage>>;
pub type OutboundSender = Sender<TransportMessage>;

pub type OutboundReceiver = Receiver<PacketProcessingMessage>;
pub type InboundSender = Sender<Result<ManagerMessage>>;

/// packages the channels needed for the inbound task
pub struct InboundChannels {
    pub from_transport: InboundReceiver,
    pub to_manager: InboundSender,
}

/// packages the channels needed for the outbound task
pub struct OutboundChannels {
    pub to_transport: OutboundSender,
    pub to_manager: InboundSender,
    pub from_manager: OutboundReceiver,
}

pub struct PacketProcessorChannels {
    pub from_manager: OutboundReceiver,
    pub to_manager: InboundSender,
    pub from_transport: InboundReceiver,
    pub to_transport: OutboundSender,
}

/// Represents a serialized packet with minimal data necessary for the transport layer.
/// Contains the encrypted packet data along with metadata needed for transmission
/// and retransmission logic. Uses Vec<u8> since it can represent any packet type.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcessedPacket {
    pub dest_addr: SocketAddr,
    pub packet_type: PacketType,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}
