use std::{net::SocketAddr, sync::Arc};

use crate::{
    dispatch,
    manager::{
        packets::types::{
            BatchID, OptionFlags, Packet, PacketFingerprint, PacketType, PacketWrapper,
            ParityPacket, SessionId, Timestamp,
        },
        types::{EncryptionMonitor, PendingAckMonitor},
    },
    packet_processor::{
        encryption::{self, Encryptable},
        fec::{self, FECCompatible},
        serialize::Serialize,
        types::{InboundSender, OutboundSender, ProcessedPacket},
    },
    prelude::*,
    unwrap_or_return,
};

use super::types::OutboundChannels;

macro_rules! serialize {
    ($packet:ident -> $buffer:ident) => {
        $buffer = vec![0u8; $packet.sized()];
        _ = $packet.serialize(&mut $buffer);
    };
}

macro_rules! processed {
    ($serialized:ident to $addr:ident as $packet_type:ident $duplicate:literal times) => {
        ProcessedPacket {
            dest_addr: $addr,
            packet_type: PacketType::$packet_type,
            data: $serialized,
            duplicate_count: $duplicate,
        }
    };
}

macro_rules! add_ack {
    (for $packet_type:ident($packet:ident), sent to $addr:ident, saved to $pending_ack_monitor:ident) => {
        if $packet.opts.contains(OptionFlags::RequireAck) {
            add_pending_ack(
                PacketWrapper {
                    addr: $addr,
                    packet: Packet::$packet_type($packet.clone()),
                },
                $packet.timestamp,
                $pending_ack_monitor,
            )
            .await
        }
    };
}

pub async fn init(
    OutboundChannels {
        t_sender,
        p_sender,
        mut p_receiver,
    }: OutboundChannels,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    pending_ack_monitor: &'static PendingAckMonitor<'_>,
) -> ErrResult {
    // initialize handle monitor
    let monitor = Arc::from(HandleMonitor::default());
    tokio::spawn(HandleMonitor::init(monitor.clone()));

    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = p_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return Err(ChannelError::ChannelClosed(Outbound).into());
        }

        let mut packets = Vec::with_capacity(received);

        for msg in buffer {
            match msg {
                PacketProcessingMessage::SendPacket(packet_wrapper) => packets.push(packet_wrapper),
                PacketProcessingMessage::Recover(session_id, batch_id) => {
                    dispatch!(recover(session_id, batch_id, p_sender.clone()) => monitor);
                }
                PacketProcessingMessage::Close => {
                    monitor.flush().await;
                    _ = t_sender.send(TransportMessage::Close).await;
                    return Ok(());
                }
                PacketProcessingMessage::ReceivedPacket(_) => unreachable!(
                    "Invariant broken while receiving from Manager:\
                received a `ReceivedPacket` variant."
                ),
                PacketProcessingMessage::Closed => unreachable!(
                    "Invariant broken while receiveing from Manager:\
                    received a `Closed` variant."
                ),
            }
        }

        dispatch!(
            handle_received(
                packets.into(),
                p_sender.clone(),
                t_sender.clone(),
                monitor.clone(),
                encryption_monitor,
                pending_ack_monitor
            ) => monitor
        );
    }
}

#[allow(clippy::unused_async)]
async fn handle_received(
    buffer: Box<[PacketWrapper]>,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    handle_monitor: Arc<HandleMonitor>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    pending_ack_monitor: &'static PendingAckMonitor<'_>,
) {
    for packet in buffer {
        dispatch!(
            handle_packet(
                packet,
                p_sender.clone(),
                t_sender.clone(),
                encryption_monitor,
                handle_monitor.clone(),
                pending_ack_monitor
            ) => handle_monitor
        );
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_packet(
    packet: PacketWrapper,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    handle_monitor: Arc<HandleMonitor>,
    pending_ack_monitor: &'static PendingAckMonitor<'_>,
) {
    let addr = packet.addr;
    let processed = match packet.packet {
        // fec + encrypted
        // could be acked
        Packet::DataPacket(packet) => {
            add_ack!(for DataPacket(packet), sent to addr, saved to pending_ack_monitor);

            dispatch!(handle_fec(
                *packet.clone(),
                addr,
                p_sender.clone(),
                t_sender.clone(),
                encryption_monitor,
                handle_monitor.clone()) => handle_monitor
            );

            let session_id = packet.session_id;
            process_encrypted(packet, session_id, addr, encryption_monitor)
        }

        //encrypted
        Packet::TrackRequestPacket(packet) => {
            add_ack!(for TrackRequestPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            process_encrypted(packet, session_id, addr, encryption_monitor)
        }
        Packet::AppRejectErrorPacket(packet) => {
            add_ack!(for AppRejectErrorPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            process_encrypted(packet, session_id, addr, encryption_monitor)
        }

        // authenticated
        Packet::RetransmitPacket(packet) => {
            add_ack!(for RetransmitPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor)
        }
        Packet::PlaybackStatusPacket(packet) => {
            add_ack!(for PlaybackStatusPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor)
        }
        Packet::SessionDoesNotExistErrorPacket(packet) => {
            add_ack!(
                for SessionDoesNotExistErrorPacket(packet),
                sent to addr,
                saved to pending_ack_monitor
            );

            let session_id = packet.session_id;
            process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor)
        }
        // could not be acked
        Packet::UnexpectedPacketErrorPacket(packet) => {
            let session_id = packet.session_id;
            process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor)
        }
        Packet::AckPacket(packet) => {
            let session_id = packet.session_id;
            process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor)
        }

        // nothing
        Packet::HelloPacket(packet) => {
            let mut serialized;
            serialize!(packet -> serialized);

            ProcessedPacket {
                dest_addr: addr,
                packet_type: PacketType::Host,
                data: serialized,
                duplicate_count: 7,
            }
        }
        Packet::IncompatibleVersion(packet) => {
            let mut serialized;
            serialize!(packet -> serialized);

            ProcessedPacket {
                dest_addr: addr,
                packet_type: PacketType::Host,
                data: serialized,
                duplicate_count: 7,
            }
        }

        // later
        Packet::MetadataPacket(_) => unimplemented!(),

        // never
        Packet::ParityPacket(_) => {
            unreachable!("A ParityPacket must never be sent from the manager layer");
        }
    };

    send_packet_to_transport(processed, t_sender, p_sender).await;
}

fn process_encrypted(
    mut packet: Box<impl Encryptable + Serialize>,
    session_id: SessionId,
    addr: SocketAddr,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> ProcessedPacket {
    encryption::encrypt(packet.as_mut(), session_id, encryption_monitor);

    let mut serialized;
    serialize!(packet -> serialized);
    processed!(serialized to addr as Error 3 times)
}

fn process_authenticated(
    packet: &impl Serialize,
    session_id: SessionId,
    addr: SocketAddr,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> ProcessedPacket {
    let mut serialized;
    serialize!(packet -> serialized);

    encryption::tag(&mut serialized, session_id, encryption_monitor);
    processed!(serialized to addr as Error 3 times)
}

async fn recover(session_id: SessionId, batch_id: BatchID, sender: InboundSender) {
    let message = match fec::recover(batch_id, session_id).await {
        Some(recovered) => Ok(ManagerMessage::Recovered(recovered)),
        None => Err(PacketProcessingError::RecoveryNotReady(session_id, batch_id).into()),
    };

    send_to_manager(message, sender).await;
}

async fn add_pending_ack(
    packet: PacketWrapper,
    timestamp: Timestamp,
    pending_ack_monitor: &'static PendingAckMonitor<'_>,
) {
    let fingerprint: PacketFingerprint = unwrap_or_return!((&packet.packet).try_into());

    pending_ack_monitor
        .add(fingerprint, (packet, timestamp))
        .await;
}

async fn handle_fec(
    packet: impl FECCompatible,
    dest_addr: SocketAddr,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    handle_monitor: Arc<HandleMonitor>,
) {
    if let Some(parity_packets) = fec::sent(packet).await {
        for packet in parity_packets {
            dispatch!(handle_parity(packet, dest_addr, t_sender.clone(), p_sender.clone(), encryption_monitor) => handle_monitor);
        }
    }
}

#[allow(clippy::unused_async)] // async is for spawning the function
async fn handle_parity(
    mut packet: ParityPacket,
    dest_addr: SocketAddr,
    t_sender: OutboundSender,
    p_sender: InboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) {
    let mut buffer;
    let session_id = packet.session_id;
    encryption::encrypt(&mut packet, session_id, encryption_monitor);
    serialize!(packet -> buffer);
    let processed_packet = ProcessedPacket {
        dest_addr,
        packet_type: PacketType::Parity,
        data: buffer,
        duplicate_count: 1,
    };

    send_packet_to_transport(processed_packet, t_sender, p_sender).await;
}

async fn send_to_manager(msg: Result<ManagerMessage>, p_sender: InboundSender) {
    _ = p_sender.send(msg).await;
}

async fn send_packet_to_transport(
    packet: ProcessedPacket,
    t_sender: OutboundSender,
    p_sender: InboundSender,
) {
    if t_sender
        .send(TransportMessage::SendPacket(packet))
        .await
        .is_err()
    {
        send_to_manager(Err(ChannelError::ChannelFailed(Outbound).into()), p_sender).await;
    }
}
