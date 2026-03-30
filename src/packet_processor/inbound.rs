use crate::{
    manager::types::EncryptionMonitor,
    packet_processor::{encryption, serialize::Serialize},
    packetizer::{fingerprint::Headers, types::*},
    prelude::*,
};

use super::types::{InboundChannels, PacketWrapper, ReceivedPacket};

use tokio::sync::mpsc::Sender;

const SESSION_ID_OFFSET: usize = 8;
const PACKET_TYPE_OFFSET: usize = 4;
const SECONDARY_TYPE_OFFSET: usize = 5;

pub async fn init(
    InboundChannels {
        mut t_receiver,
        p_sender,
    }: InboundChannels,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> ErrResult {
    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = t_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return Err(ChannelError::ChannelClosed(Inbound).into());
        }
        handle_messages(buffer.into(), p_sender.clone(), encryption_monitor);
    }
}

async fn handle_messages(
    buffer: Box<[Result<ReceivedPacket>]>,
    sender: Sender<Result<PacketWrapper>>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) {
    for packet in buffer {
        let packet = match packet {
            Ok(packet) => packet,
            Err(err) => {
                if !err.is_recoverable() {
                    tokio::spawn(send_up(Err(err), sender.clone()));
                    continue;
                } else {
                    unreachable!("No pacth currently leads to a recoverable error at this level");
                }
            }
        };

        tokio::spawn(handle_packet(packet, sender.clone(), encryption_monitor));
    }
}

async fn handle_packet(
    mut packet: ReceivedPacket,
    sender: Sender<Result<PacketWrapper>>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) {
    let Ok(version) = Version::deserialize(&packet.data) else {
        return;
    };

    if version.is_zero()
        && let Ok(packet) = IncompatibleVersion::deserialize(&packet.data)
    {
        tokio::spawn(send_up(
            Ok(PacketWrapper::IncompatibleVersion(Box::new(packet))),
            sender.clone(),
        ));
        return;
    }

    if !version.is_compatible() {
        tokio::spawn(send_up(
            Err(PacketProcessingError::IncompatibleVersion(version).into()),
            sender.clone(),
        ));
    }

    // opts is 16 bits, but isnt necessary to check right now
    let Ok(packet_type) = <PacketType>::deserialize(&packet.data[PACKET_TYPE_OFFSET..]) else {
        return;
    };

    let Ok(packet) = deserialize_and_decrypt(packet_type, &mut packet.data, encryption_monitor)
    else {
        return;
    };

    send_up(Ok(packet), sender.clone()).await;
}

fn handle_incompatible_version_packet(
    data: &mut Vec<u8>,
) -> core::result::Result<IncompatibleVersion, ()> {
    IncompatibleVersion::deserialize(data)
}

fn deserialize_and_decrypt(
    packet_type: PacketType,
    data: &mut Vec<u8>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> core::result::Result<PacketWrapper, ()> {
    match packet_type {
        // Single types
        PacketType::Data => {
            let mut packet = Box::new(DataPacket::deserialize(data)?);
            if !encryption::decrypt(
                encryption::Encryptable::Data(&mut packet),
                encryption_monitor,
            ) {
                Err(())
            } else {
                Ok(PacketWrapper::DataPacket(packet))
            }
        }
        PacketType::Parity => {
            let mut packet = Box::new(ParityPacket::deserialize(data)?);
            if !encryption::decrypt(
                encryption::Encryptable::Parity(&mut packet),
                encryption_monitor,
            ) {
                Err(())
            } else {
                Ok(PacketWrapper::ParityPacket(packet))
            }
        }
        PacketType::Ack => {
            authenticate(data, encryption_monitor)?;
            let mut packet = PacketWrapper::AckPacket(Box::new(AckPacket::deserialize(data)?));

            Ok(packet)
        }

        // Subtypes
        PacketType::Host | PacketType::Session | PacketType::Playback => {
            deserialize_and_auth_control_packet(data, encryption_monitor)
        }
        PacketType::Error => deserialize_error_packet(data, encryption_monitor),

        // Not yet
        PacketType::Metadata => unimplemented!(),
    }
}

fn deserialize_and_auth_control_packet(
    data: &mut Vec<u8>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> core::result::Result<PacketWrapper, ()> {
    authenticate(data, encryption_monitor)?;

    Ok(
        match ControlType::deserialize(&data[SECONDARY_TYPE_OFFSET..])? {
            // host
            ControlType::Host(HostControlType::Hello) => {
                PacketWrapper::HelloPacket(Box::new(HelloPacket::deserialize(data)?))
            }
            //session
            ControlType::Session(SessionControlType::Retransmit) => {
                PacketWrapper::RetransmitPacket(Box::new(RetransmitPacket::deserialize(data)?))
            }
            ControlType::Session(SessionControlType::TrackRequest) => {
                PacketWrapper::TrackRequestPacket(Box::new(TrackRequestPacket::deserialize(data)?))
            }
            ControlType::Session(SessionControlType::MetadataRequest) => unimplemented!(),
            //playback
            ControlType::Playback(_) => PacketWrapper::PlaybackStatusPacket(Box::new(
                PlaybackStatusPacket::deserialize(data)?,
            )),
        },
    )
}

fn deserialize_error_packet(
    data: &mut Vec<u8>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> core::result::Result<PacketWrapper, ()> {
    authenticate(data, encryption_monitor)?;

    Ok(
        match ErrorType::deserialize(&data[SECONDARY_TYPE_OFFSET..])? {
            ErrorType::AppReject => PacketWrapper::AppRejectErrorPacket(Box::new(
                AppRejectErrorPacket::deserialize(data)?,
            )),
            ErrorType::UnexpectedPacket | ErrorType::IncomprehensiblePacket => {
                PacketWrapper::UnexpectedPacketErrorPacket(Box::new(
                    UnexpectedPacketErrorPacket::deserialize(data)?,
                ))
            }
            ErrorType::SessionDoesNotExist => PacketWrapper::SessionDoesNotExistErrorPacket(
                Box::new(SessionDoesNotExistErrorPacket::deserialize(data)?),
            ),
        },
    )
}

fn authenticate(
    packet: &mut Vec<u8>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> core::result::Result<(), ()> {
    let session_id = SessionId::deserialize(&packet[SESSION_ID_OFFSET..])?;
    if !encryption::authenticate(packet, session_id, encryption_monitor) {
        Err(())
    } else {
        Ok(())
    }
}

async fn send_up(message: Result<PacketWrapper>, sender: Sender<Result<PacketWrapper>>) {
    sender.send(message).await;
}
