#![allow(unused_variables)]

use std::sync::OnceLock;

use crate::{
    manager::types::EncryptionMonitor,
    packet_processor::{encryption, serialize::Serialize},
    packetizer::{
        fingerprint::Headers,
        types::{
            AckPacket, AppRejectErrorPacket, ControlType, DataPacket, ErrorType, HelloPacket,
            HostControlType, Options, PacketType, ParityPacket, PlaybackControlType,
            PlaybackStatusPacket, RetransmitPacket, SessionControlType,
            SessionDoesNotExistErrorPacket, TrackRequestPacket, UnexpectedPacketErrorPacket,
            Version,
        },
    },
    prelude::*,
};

use super::types::{InboundChannels, PacketWrapper, ReceivedPacket};

use tokio::sync::mpsc::Sender;

static ENCRYPTION_MONITOR: OnceLock<&EncryptionMonitor> = OnceLock::new();

pub async fn init(
    InboundChannels {
        mut t_receiver,
        p_sender,
    }: InboundChannels,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> ErrResult {
    ENCRYPTION_MONITOR.set(encryption_monitor);
    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = t_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return Err(ChannelError::ChannelClosed(Inbound).into());
        }
        handle_messages(buffer.into(), p_sender.clone());
    }
}

fn get_encryption_monitor() -> &'static EncryptionMonitor<'static> {
    ENCRYPTION_MONITOR.get().expect(
        "Invariant broken while accessing ENCRYPTION_MONITOR: \
    ENCRYPTION_MONITOR not initialized",
    )
}

async fn handle_messages(
    buffer: Box<[Result<ReceivedPacket>]>,
    sender: Sender<Result<PacketWrapper>>,
) {
    for packet in buffer {
        // TODO: Erorr handling
        let packet = packet.expect("Error handling");
    }
}

async fn handle_packet(packet: ReceivedPacket, sender: Sender<Result<PacketWrapper>>) {
    if packet.data.len() < 2 {
        return;
    }

    let Some(version) = Version::deserialize(&packet.data) else {
        return;
    };

    if !version.is_compatible() {
        todo!("VersionIncompatible Error");
    }

    // opts is 16 bits, but isnt necessary to check right now
    let Some((_, packet_type)) = <(u16, PacketType)>::deserialize(&packet.data[2..]) else {
        return;
    };

    let Some(packet) = deserialize(packet_type, &packet.data) else {
        todo!("IncoherentPacket Error");
    };
}

fn deserialize(packet_type: PacketType, data: &[u8]) -> Option<PacketWrapper> {
    match packet_type {
        // Single types
        PacketType::Data => Some(PacketWrapper::DataPacket(DataPacket::deserialize(&data)?)),
        PacketType::Parity => Some(PacketWrapper::ParityPacket(ParityPacket::deserialize(
            &data,
        )?)),
        PacketType::Ack => Some(PacketWrapper::AckPacket(AckPacket::deserialize(&data)?)),

        // Subtypes
        PacketType::Host | PacketType::Session | PacketType::Playback => {
            deserialize_control_packet(data)
        }
        PacketType::Error => deserialize_error_packet(data),

        // Not yet
        PacketType::Metadata => unimplemented!(),
    }
}

fn deserialize_control_packet(data: &[u8]) -> Option<PacketWrapper> {
    Some(match ControlType::deserialize(&data[5..])? {
        ControlType::Host(host_control_type) => match host_control_type {
            HostControlType::Hello => PacketWrapper::HelloPacket(HelloPacket::deserialize(data)?),
        },
        ControlType::Session(session_control_type) => match session_control_type {
            SessionControlType::Retransmit => {
                PacketWrapper::RetransmitPacket(RetransmitPacket::deserialize(data)?)
            }
            SessionControlType::TrackRequest => {
                PacketWrapper::TrackRequestPacket(TrackRequestPacket::deserialize(data)?)
            }
            SessionControlType::MetadataRequest => unimplemented!(),
        },
        ControlType::Playback(_) => {
            PacketWrapper::PlaybackStatusPacket(PlaybackStatusPacket::deserialize(data)?)
        }
    })
}

fn deserialize_error_packet(data: &[u8]) -> Option<PacketWrapper> {
    Some(match ErrorType::deserialize(&data[5..])? {
        ErrorType::AppReject => {
            PacketWrapper::AppRejectErrorPacket(AppRejectErrorPacket::deserialize(data)?)
        }
        ErrorType::UnexpectedPacket | ErrorType::IncomprehensiblePacket => {
            PacketWrapper::UnexpectedPacketErrorPacket(UnexpectedPacketErrorPacket::deserialize(
                data,
            )?)
        }
        ErrorType::SessionDoesNotExist => PacketWrapper::SessionDoesNotExistErrorPacket(
            SessionDoesNotExistErrorPacket::deserialize(data)?,
        ),
    })
}

fn decrypt_packet(packet: encryption::Encryptable) -> bool {
    match packet {
        encryption::Encryptable::Data(data_packet) => todo!(),
        encryption::Encryptable::Parity(parity_packet) => todo!(),
    }
}
