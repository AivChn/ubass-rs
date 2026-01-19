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
