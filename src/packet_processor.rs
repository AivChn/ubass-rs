use futures::channel::mpsc::Sender;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    usize,
};
use tokio::sync::mpsc::Receiver;

use crate::{
    InternalError,
    packetizer::{
        AckPacket, ControlPacket, DataPacket, MAX_PAYLOAD_LENGTH, Options, PacketType,
        PacketTypeFecBatchID, PacketWrapper, SessionId, Version,
    },
    transport::{ReceivedPacket, TransportError},
};

mod ReedSolomonFEC {
    use crate::{
        packet_processor::{Batch, FecPacket, ProcessedPacket},
        packetizer::{DataPacket, PacketType, PacketWrapper, ParityPacket, SessionId},
    };
    use reed_solomon_simd;
    use serde::de::value;
    use std::{
        collections::{HashMap, HashSet, btree_map::Entry},
        sync::LazyLock,
        time::{SystemTime, UNIX_EPOCH},
    };
    use tokio::sync::Mutex;

    struct BatchFull;

    struct ToSendBatch {
        packets: Vec<FecPacket>,
        batch_id: u16,
        batch_size: u8,
        batch_top: u8,
    }

    impl ToSendBatch {
        fn new() -> Self {
            Self {
                packets: Vec::new(),
                batch_id: Self::get_batch_id(),
                batch_size: Self::get_batch_size(),
                batch_top: 0,
            }
        }

        fn get_batch_id() -> u16 {
            // ignore this i just felt like it
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("TIME")
                .as_millis()
                * 3_432_141_324) as u16
                ^ ((69_567_564_845 >> 33) & 2_134_123_512) as u16
        }

        fn get_batch_size() -> u8 {
            24
        }
    }

    impl From<DataPacket> for FecPacket {
        fn from(value: DataPacket) -> Self {
            let mut data: Vec<u8> = vec![];
            data.extend(value.byte_range_start.to_be_bytes());
            data.extend(value.byte_range_offset.to_be_bytes());
            data.extend(value.payload_length.to_be_bytes());
            data.extend(value.payload);
            Self {
                is_data: value.packet_type_batch_id.0 == PacketType::Data,
                batch_pos: value.fec_info.batch_pos,
                data: value.payload.into(),
            }
        }
    }

    impl AsRef<[u8]> for FecPacket {
        fn as_ref(&self) -> &[u8] {
            self.data.as_ref()
        }
    }

    static RECEIVED_PACKETS: LazyLock<Mutex<HashMap<Batch, Option<HashSet<FecPacket>>>>> =
        LazyLock::new(Default::default);

    static TO_SEND: LazyLock<Mutex<HashMap<SessionId, ToSendBatch>>> =
        LazyLock::new(Default::default);

    async fn received(batch: Batch, pack: FecPacket) -> Result<(), BatchFull> {
        let mut table = RECEIVED_PACKETS.lock().await;
        let batch_size = batch.batch_size as usize;
        let entry = table.entry(batch).or_insert(Some(HashSet::new()));

        if let Some(entry) = entry {
            if entry.len() <= batch_size {
                entry.insert(pack);
            } else {
                return Err(BatchFull {});
            }
        }

        Ok(())
    }

    async fn sent(packet: DataPacket) -> Option<DataPacket> {
        let mut table = TO_SEND.lock().await;
        let entry = table.entry(packet.session_id).or_insert(ToSendBatch::new());
        entry.packets.push(FecPacket::from(packet));

        if entry.packets.len() >= entry.batch_size as usize {
            // TODO: Finsh the sent() function
            todo!()
        }

        None
    }

    fn derive_parity(mut entry: ToSendBatch) -> Option<FecPacket> {
        entry
            .packets
            .sort_by(|p1, p2| p1.batch_pos.cmp(&p2.batch_pos));
        let Ok(value) = reed_solomon_simd::encode(
            entry.batch_size as usize,
            (entry.batch_size / 3) as usize,
            entry.packets,
        ) else {
            return None;
        };

        //TODO: FIX EVERYTHING
        let parity_packets = value.iter().map(|p| ParityPacket::new(p.try_into()));

        None
    }
}

// =================== TYPE DEFINITIONS =================================|

struct PacketIdentifiers {
    session_id: SessionId,
    packet_type: PacketType,
    opts: Options,
    timestamp_ms: u64,
}

#[repr(C)]
#[derive(Hash, PartialEq, Eq)]
struct Batch {
    batch_id: u16,
    batch_size: u8,
}

#[derive(Hash, Eq)]
struct FecPacket {
    is_data: bool,
    batch_pos: u8,
    data: [u8; MAX_PAYLOAD_LENGTH],
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
pub enum TransportSendMessage {
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

/// Errors that can occur during packet processing operations.
/// Covers deserialization failures, version incompatibilities, and internal errors.
#[derive(Debug)]
pub enum PacketProcessingError {
    Internal(InternalError),
    PacketTypeNotIMplemented(PacketType),
    IncompatibleVersion(Version),
    WrongHeaderSize(usize),
    InvalidPacketTypeHeader(u8),
    FailedToDeserialize,
}

// =================== PUBLIC FUNCTIONS =================================|

/// Initializes the packet processing layer and supervises send/recv tasks.
///
/// Spawns two concurrent tasks:
/// - recv: Handles incoming packets from transport layer, processes and forwards to packetizer
/// - send: Handles outgoing packets from packetizer, processes and forwards to transport
///
/// Acts as a supervisor, monitoring both tasks and handling failures.
/// If either task fails, the supervisor will abort the other and return an error.
pub async fn init(
    p_receiver: Receiver<PacketProcessingMessage>,
    p_sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
    t_receiver: Receiver<Result<ReceivedPacket, TransportError>>,
    t_sender: Sender<TransportSendMessage>,
    fec_table: Arc<HashMap<Batch, HashSet<FecPacket>>>,
) -> Result<(), PacketProcessingError> {
    let mut recv_handle = tokio::spawn(recv(t_receiver, p_sender.clone(), fec_table));
    let mut send_handle = tokio::spawn(send(t_sender.clone(), p_sender.clone(), p_receiver));

    'supervisor: loop {
        _ = tokio::select! {
            res = &mut recv_handle, if !recv_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(PacketProcessingError::Internal(InternalError::TaskFailed));
                };

                match result {
                    // TODO: update error handling
                    Err(e) => Err::<(), _>(e),
                    Ok(()) => break 'supervisor Ok(()),
                }
            },
            res = &mut send_handle, if !send_handle.is_finished() => {
                let Ok(result) = res else {
                    break 'supervisor Err(PacketProcessingError::Internal(InternalError::TaskFailed));
                };

                match result {
                    // TODO: update error handling
                    Err(e) => Err(e),
                    Ok(()) => { recv_handle.abort(); break 'supervisor Ok(())},
                }
            }
        }
    }
}

// =================== PRIVATE IMPLEMENTATION =================================|

/// Receive pipeline: handles incoming packets from transport layer.
///
/// Processing steps (planned):
/// 1. Receive encrypted packet from transport
/// 2. Decrypt packet data
/// 3. For data/parity packets:
///    - Save to FEC table
///    - If batch complete: discard batch
///    - If wait time exceeded:
///      - If enough packets to derive: use FEC
///      - Otherwise: request retransmission of missing ranges
/// 4. Deserialize packet
/// 5. Forward to packetizer layer
async fn recv(
    mut t_receiver: Receiver<Result<ReceivedPacket, TransportError>>,
    p_sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
    fec_table: Arc<HashMap<Batch, HashSet<FecPacket>>>,
) -> Result<(), PacketProcessingError> {
    // wait on receive
    loop {
        let packet = match t_receiver.recv().await {
            Some(Ok(packet)) => packet,
            Some(Err(e)) => todo!("ERROR HANDLING!!!!!"),
            None => {
                return Err(PacketProcessingError::Internal(
                    InternalError::ChannelClosed,
                ));
            }
        };

        let data = tokio::spawn(process_received_packet(
            packet,
            p_sender.clone(),
            fec_table.clone(),
        ));
    }
    // deserialize

    // send to packetizer
}

/// Send pipeline: handles outgoing packets from packetizer to transport.
///
/// Processing steps (planned):
/// 1. Wait for packet from packetizer
/// 2. Serialize packet
/// 3. Save copy for parity derivation
/// 4. Encrypt packet data
/// 5. Send to transport layer
/// 6. If final packet in batch:
///    - Derive parity packets
///    - Send parity packets
///    - Calculate new batch size for adaptive FEC
async fn send(
    t_sender: Sender<TransportSendMessage>,
    p_sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
    p_receiver: Receiver<PacketProcessingMessage>,
) -> Result<(), PacketProcessingError> {
    // TODO: implement send pipeline
    todo!("implement send pipeline")

    // Wait on receive

    //serialize

    // save copy for parity derivition

    // encrypt

    // send to transport

    // if final packet in batch
    //  derive parity
    //  send parity
    //  calculate new batch size

    // repeat
}

// =================== HELPER FUNCTIONS =================================|

/// Processes a received packet through the full receive pipeline.
///
/// Steps:
/// 1. Process serialized packet (validate headers, extract metadata)
/// 2. Retrieve encryption key for the session
/// 3. Decrypt packet data
/// 4. Deserialize into typed PacketWrapper
async fn process_received_packet(
    received_packet: ReceivedPacket,
    sender: Sender<Result<PacketWrapper, PacketProcessingError>>,
    fec_table: Arc<HashMap<Batch, HashSet<FecPacket>>>,
) -> Result<(), PacketProcessingError> {
    let processed = match process_serialized(received_packet) {
        Ok(packet) => packet,
        Err(err) => todo!("error handling"),
    };

    let key = get_key_from_session(processed.packet_id.session_id)
        .expect("literal value returned as Some");
    let decrypted_data = decrypt(processed.data, key);

    let processed = ProcessedPacket {
        packet_id: processed.packet_id,
        packet_type: processed.packet_type,
        data: decrypted_data,
        duplicate_count: processed.duplicate_count,
    };

    let packet = match deserialize(processed) {
        Ok(packet) => packet,
        Err(err) => todo!("error handling"),
    };

    match packet {
        PacketWrapper::DataPacket(pack) => {
            let batch = Batch {
                batch_id: pack.packet_type_batch_id.1,
                batch_size: pack.fec_info.batch_size,
            };
            fec_table
                .entry(batch)
                .or_insert(HashSet::from([pack.into()]))
        }
        _ => todo!(),
    };

    // decrypt

    // == if data or parity ==

    // save to FEC table

    // === if batch ended ===

    // discard batch

    // === else ===

    // ==== if packet wait time exceeded ====

    // ===== if there is enough packets to derive =====

    // use FEC

    // ===== else =====

    // send to packetizer missing ranges

    // == else ==

    // save identifying info

    // == end ==

    Ok(())
}

/// Converts a PacketWrapper into a ProcessedPacket ready for transmission.
///
/// Serializes the packet, extracts metadata, and prepares it for the transport layer.
/// Different packet types have different duplicate count defaults based on their importance.
fn process_packet(packet: PacketWrapper) -> ProcessedPacket {
    match packet {
        PacketWrapper::DataPacket(pack) => {
            // TODO: handle serialization error
            let data = wincode::serialize(&pack).expect("I didnt handle this yet")
                [..pack.payload_length as usize + DataPacket::HEADER_SIZE]
                .to_vec();

            ProcessedPacket {
                packet_id: PacketId {
                    timestamp: pack.timestamp_ms,
                    session_id: pack.session_id,
                },
                packet_type: pack.packet_type_batch_id.0,
                data,
                duplicate_count: 1,
            }
        }
        PacketWrapper::AckPacket(pack) => {
            // TODO: handle serialization error
            let data = wincode::serialize(&pack).expect("I didnt handlet this yet");

            ProcessedPacket {
                packet_id: PacketId {
                    timestamp: pack.timestamp_ms,
                    session_id: pack.session_id,
                },
                packet_type: pack.packet_type,
                data,
                duplicate_count: 5,
            }
        }
        PacketWrapper::ControlPacket(pack) => {
            // TODO: handle serialization error
            let data = wincode::serialize(&pack).expect("I didnt handle this yet")
                [..pack.payload_length as usize + ControlPacket::HEADER_SIZE]
                .to_vec();

            ProcessedPacket {
                packet_id: PacketId {
                    timestamp: pack.timestamp_ms,
                    session_id: pack.session_id,
                },
                packet_type: pack.packet_type,
                data,
                duplicate_count: 3,
            }
        }
    }
}
/// Retrieves the encryption key for a given session.
/// Currently returns a placeholder key - will be implemented with proper key management.
fn get_key_from_session(session_id: SessionId) -> Option<u128> {
    Some(34215909873652376164537433124 as u128)
}

/// Decrypts packet data using the provided key.
/// Currently a no-op placeholder - encryption implementation pending.
fn decrypt(packet: Vec<u8>, key: u128) -> Vec<u8> {
    _ = key;
    packet
}

/// Serializes a PacketWrapper into raw bytes.
/// Handles different packet types appropriately.
fn serialize_packet(wrapped_packet: PacketWrapper) -> Option<Vec<u8>> {
    match wrapped_packet {
        PacketWrapper::DataPacket(packet) => {
            if let Ok(serialized) = wincode::serialize(&packet) {
                Some(
                    serialized[..packet.payload_length as usize + DataPacket::HEADER_SIZE].to_vec(),
                )
            } else {
                None
            }
        }
        PacketWrapper::ControlPacket(packet) => {
            if let Ok(serialized) = wincode::serialize(&packet) {
                Some(
                    serialized[..packet.payload_length as usize + ControlPacket::HEADER_SIZE]
                        .to_vec(),
                )
            } else {
                None
            }
        }
        PacketWrapper::AckPacket(packet) => wincode::serialize(&packet).ok(),
    }
}

/// Processes raw serialized packet data into a ProcessedPacket.
///
/// Validates and extracts:
/// - Protocol version (checks compatibility)
/// - Options flags
/// - Packet type
/// - Session ID
///
/// Performs header size validation based on packet type.
fn process_serialized(packet: ReceivedPacket) -> Result<ProcessedPacket, PacketProcessingError> {
    if packet.data.len() < 5 {
        return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
    }

    let packet_version = Version::from_bytes(
        packet.data[0..=1]
            .try_into()
            .expect("already guranteed size"),
    );

    if !packet_version.is_compatible() {
        return Err(PacketProcessingError::IncompatibleVersion(packet_version));
    }

    // bytes 2,3 are opts, not needed

    let Ok(packet_type) = PacketType::try_from(packet.data[4]) else {
        return Err(PacketProcessingError::InvalidPacketTypeHeader(
            packet.data[4],
        ));
    };

    match packet_type {
        PacketType::Data | PacketType::Parity => {
            if packet.data.len() < DataPacket::MIN_SIZE {
                return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
            }
        }
        PacketType::Control => {
            if packet.data.len() < ControlPacket::MIN_SIZE {
                return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
            }
        }
        PacketType::Ack => {
            if packet.data.len() < AckPacket::MIN_SIZE {
                return Err(PacketProcessingError::WrongHeaderSize(packet.data.len()));
            }
        }
        // TODO: implement the rest after adding the packets
        _ => return Err(PacketProcessingError::PacketTypeNotIMplemented(packet_type)),
    };

    let session_id = SessionId::from_bytes(
        packet.data[6..14]
            .try_into()
            .expect("an 8 byte slice is the same as an 8 byte array"),
    );

    Ok(ProcessedPacket {
        packet_id: PacketId {
            timestamp: 0,
            session_id: session_id,
        },
        packet_type,
        data: packet.data,
        duplicate_count: 1,
    })
}

/// Deserializes a ProcessedPacket into a typed PacketWrapper.
///
/// Resizes the data buffer to match expected packet size before deserialization.
/// Different packet types require different buffer sizes based on their structure.
fn deserialize(mut packet: ProcessedPacket) -> Result<PacketWrapper, PacketProcessingError> {
    match packet.packet_type {
        PacketType::Data | PacketType::Parity => {
            packet
                .data
                .resize(DataPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            let Ok(deserialized) = wincode::deserialize::<DataPacket>(&packet.data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::DataPacket(deserialized))
        }
        PacketType::Control => {
            packet
                .data
                .resize(ControlPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH, 0);
            let Ok(deserialized) = wincode::deserialize::<ControlPacket>(&packet.data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::ControlPacket(deserialized))
        }
        PacketType::Ack => {
            packet.data.resize(AckPacket::HEADER_SIZE, 0);
            let Ok(deserialized) = wincode::deserialize::<AckPacket>(&packet.data) else {
                return Err(PacketProcessingError::FailedToDeserialize);
            };

            Ok(PacketWrapper::AckPacket(deserialized))
        }

        // TODO: implement the rest after adding the packets
        _ => panic!("Havent taken care of this yet"),
    }
}

impl PartialEq for FecPacket {
    fn eq(&self, other: &Self) -> bool {
        self.batch_pos == other.batch_pos && self.is_data == other.is_data
    }
}

impl Into<FecPacket> for DataPacket {
    fn into(self) -> FecPacket {
        if self.packet_type_batch_id.0 == PacketType::Data
            || self.packet_type_batch_id.0 == PacketType::Parity
        {
            FecPacket {
                is_data: self.packet_type_batch_id.0 == PacketType::Data,
                batch_pos: self.fec_info.batch_pos,
                data: self.payload,
            }
        } else {
            unreachable!()
        }
    }
}
