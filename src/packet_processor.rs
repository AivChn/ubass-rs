use crate::packetizer::PacketType;

#[derive(Debug, Clone, Copy)]
pub struct PacketId {
    pub id: u128,
    pub session_token: u128,
}

pub struct ProcessedPacket {
    pub packet_id: PacketId,
    pub packet_type: PacketType,
    pub data: Vec<u8>,
    pub duplicate_count: usize,
}
