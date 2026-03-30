use std::{process::Output, sync::Arc};

use tokio::sync::mpsc::{Receiver, Sender};

use crate::{
    dispatch,
    manager::types::EncryptionMonitor,
    packet_processor::{
        encryption::{self, Encryptable},
        fec::{self, FECCompatible},
        serialize::{self, Serialize},
        types::{InboundSender, OutboundSender, ProcessedPacket},
    },
    packetizer::{
        fingerprint::Payload,
        types::{DataPacket, ErrorType, PacketWrapper, ParityPacket, SessionId},
    },
    prelude::*,
};

use super::types::{OutboundChannels, PacketProcessingMessage, Packets, TransportMessage};

macro_rules! serialize {
    ($packet:ident -> $buffer:ident) => {
        $buffer = vec![0u8; $packet.sized()];
        $packet.serialize(&mut $buffer);
    };
}

pub async fn init(
    OutboundChannels {
        t_sender,
        p_sender,
        mut p_receiver,
    }: OutboundChannels,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> (Receiver<PacketProcessingMessage>, ErrResult) {
    let monitor = Arc::from(HandleMonitor::default());
    tokio::spawn(HandleMonitor::init(monitor.clone()));

    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = p_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return (
                p_receiver,
                Err(ChannelError::ChannelClosed(Outbound).into()),
            );
        }

        let mut packets = Vec::with_capacity(received);

        for msg in buffer {
            let packet = match msg {
                PacketProcessingMessage::Close => return (p_receiver, Ok(())),
                PacketProcessingMessage::SendPacket(packet_wrapper) => packet_wrapper,
            };
            packets.push(packet);
        }

        dispatch!(
            handle_received(
                packets.into(),
                p_sender.clone(),
                t_sender.clone(),
                monitor.clone(),
                encryption_monitor
            ) => monitor
        );
    }
}

#[allow(clippy::unused_async)]
async fn handle_received(
    buffer: Box<[PacketWrapper]>,
    p_sender: Sender<Result<PacketWrapper>>,
    t_sender: Sender<TransportMessage>,
    handle_monitor: Arc<HandleMonitor>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) {
    // TODO: implement this!!!
    // 1. serialize
    // 2. [fec]
    // 3. encrypt
    // 4. send to transport
    //
    // - fec process:
    //  1. sent()
    //  2. if received back a packet, send it

    for packet in buffer {
        dispatch!(handle_packet(packet, p_sender.clone(), t_sender.clone(), encryption_monitor, handle_monitor.clone()) => handle_monitor);
    }
}

async fn handle_packet(
    packet: PacketWrapper,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    handle_monitor: Arc<HandleMonitor>,
) {
    let mut serialized;
    match packet.packet {
        // fec
        Packets::DataPacket(mut packet) => {
            let session_id = packet.session_id;
            dispatch!(handle_fec(
                    (*packet).clone(), 
                    p_sender.clone(), 
                    t_sender.clone(), 
                    encryption_monitor,
                    handle_monitor.clone()) => handle_monitor);
            encryption::encrypt(packet.as_mut(), session_id, encryption_monitor);
            serialize!(packet -> serialized);
        }

        //encrypted
        Packets::TrackRequestPacket(mut packet) => {
            let session_id = packet.session_id;
            encryption::encrypt(packet.as_mut(), session_id, encryption_monitor);
            serialize!(packet -> serialized);
        }
        Packets::AppRejectErrorPacket(mut packet) => {
            let session_id = packet.session_id;
            encryption::encrypt(packet.as_mut(), session_id, encryption_monitor);
            serialize!(packet -> serialized);
        }

        // authenticated
        Packets::RetransmitPacket(mut packet) => {
            let mut session_id = packet.session_id;
            serialize!(packet -> serialized);
            encryption::tag(&mut serialized, session_id, encryption_monitor);
        }
        Packets::AckPacket(mut packet) => {
            let mut session_id = packet.session_id;
            serialize!(packet -> serialized);
            encryption::tag(&mut serialized, session_id, encryption_monitor);
        }
        Packets::PlaybackStatusPacket(mut packet) => {
            let mut session_id = packet.session_id;
            serialize!(packet -> serialized);
            encryption::tag(&mut serialized, session_id, encryption_monitor);
        }
        Packets::SessionDoesNotExistErrorPacket(mut packet) => {
            let mut session_id = packet.session_id;
            serialize!(packet -> serialized);
            encryption::tag(&mut serialized, session_id, encryption_monitor);
        }
        Packets::UnexpectedPacketErrorPacket(mut packet) => {
            let mut session_id = packet.session_id;
            serialize!(packet -> serialized);
            encryption::tag(&mut serialized, session_id, encryption_monitor);
        }

        // nothing
        Packets::HelloPacket(mut packet) => {
            serialize!(packet -> serialized);
        }
        Packets::IncompatibleVersion(packet) => {
            serialize!(packet -> serialized);
        }

        // later
        Packets::MetadataPacket(mut packet) => todo!(),

        // never
        Packets::ParityPacket(_) => {
            unreachable!("A ParityPacket must never be sent from the manager layer");
        }
    }
}

async fn handle_fec(
    packet: impl FECCompatible,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    handle_monitor: Arc<HandleMonitor>,
) {
    if let Some(parity_packets) = fec::sent(packet).await {
        for packet in parity_packets {
            dispatch!(handle_parity(packet, t_sender.clone(), p_sender.clone(), encryption_monitor) => handle_monitor);
        }
    }
}

#[allow(clippy::unused_async)] // async is for spawning the function
async fn handle_parity(
    mut packet: ParityPacket,
    t_sender: OutboundSender,
    p_sender: InboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) {
    let mut buffer;
    let session_id = packet.session_id;
    encryption::encrypt(&mut packet, session_id, encryption_monitor);
    serialize!(packet -> buffer);
    let processed_packet = ProcessedPacket {
        dest_addr: todo!(),
        packet_id: todo!(),
        packet_type: todo!(),
        data: todo!(),
        duplicate_count: todo!(),
    };
}

async fn send_to_packetizer(err: Error, p_sender: InboundSender) {
    p_sender.send(Err(err)).await;
}

async fn send_packet_to_transport(
    packet: ProcessedPacket,
    t_sender: OutboundSender,
    p_sender: InboundSender,
) {
    if t_sender.send(TransportMessage::Data(packet)).await.is_err() {
        send_to_packetizer(ChannelError::ChannelFailed(Outbound).into(), p_sender).await;
    }
}
