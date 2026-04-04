use crate::{
    packet_processor::{fec::RecoverdPacket, types::ProcessedPacket},
    packetizer::types::{BatchID, PacketWrapper, SessionId},
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
