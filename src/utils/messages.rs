use crate::prelude::*;
use crate::{
    manager::packets::types::{BatchID, PacketWrapper, SessionId},
    packet_processor::{fec::RecoverdPacket, types::ProcessedPacket},
    transport::types::ReceivedPacket,
};

pub enum ManagerMessage {
    Recovered(Vec<RecoverdPacket>),
    Packet(PacketWrapper),
    Closed,
}

pub enum PacketProcessingMessage {
    SendPacket(PacketWrapper),
    ReceivedPacket(ReceivedPacket),
    Recover(SessionId, BatchID),
    Close,
    Closed,
}

pub enum TransportMessage {
    SendPacket(ProcessedPacket),
    Close,
}
