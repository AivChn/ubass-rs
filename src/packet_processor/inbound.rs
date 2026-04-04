#![allow(clippy::wildcard_imports)]

use crate::{
    manager::types::{EncryptionMonitor, FingerprintMonitor},
    packet_processor::{encryption, serialize::Serialize},
    packetizer::{fingerprint::Headers, types::*},
    prelude::*,
    transport::types::ReceivedPacket,
    unwrap_or_return,
};

use super::types::{InboundChannels, InboundSender};
use crate::packetizer::types::Packet;

const SESSION_ID_OFFSET: usize = 8;
const PACKET_TYPE_OFFSET: usize = 4;
const SECONDARY_TYPE_OFFSET: usize = 5;

pub async fn init(
    InboundChannels {
        mut t_receiver,
        p_sender,
    }: InboundChannels,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) -> ErrResult {
    loop {
        let mut buffer = Vec::with_capacity(16);
        let received = t_receiver.recv_many(&mut buffer, 16).await;
        if received == 0 {
            return Err(ChannelError::ChannelClosed(Inbound).into());
        }

        for message in buffer {
            let packet = match message {
                Ok(PacketProcessingMessage::ReceivedPacket(packet)) => packet,
                Ok(PacketProcessingMessage::Closed) => {
                    _ = p_sender.send(Ok(ManagerMessage::Closed)).await;
                    return Ok(());
                }
                Ok(_) => unreachable!(
                    "Invariant broken while receiving from Transport: a message variant other than `ReceivedPacket` and `Closed` was received."
                ),
                Err(err) => {
                    if !err.is_recoverable() {
                        tokio::spawn(send_up(Err(err), p_sender.clone()));
                        continue;
                    }
                    unreachable!("No path currently leads to a recoverable error at this level");
                }
            };

            tokio::spawn(handle_packet(
                packet,
                p_sender.clone(),
                encryption_monitor,
                fingerprint_monitor,
            ));
        }
    }
}

async fn handle_packet(
    packet: ReceivedPacket,
    sender: InboundSender,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) {
    let version = unwrap_or_return!(Version::deserialize(&packet.data));

    if version.is_zero() {
        let ready_packet = unwrap_or_return!(IncompatibleVersionPacket::deserialize(&packet.data));
        tokio::spawn(send_up(
            Ok(ManagerMessage::Packet(
                Packet::IncompatibleVersion(Box::new(ready_packet)).wrap(packet.src_addr),
            )),
            sender.clone(),
        ));
        return;
    }

    if !version.is_compatible() {
        tokio::spawn(send_up(
            Err(PacketProcessingError::IncompatibleVersion(version, packet.src_addr).into()),
            sender.clone(),
        ));
        return;
    }

    let packet_type =
        unwrap_or_return!(PacketType::deserialize(&packet.data[PACKET_TYPE_OFFSET..]));

    let ready_packet = unwrap_or_return!(
        deserialize_and_decrypt(
            packet_type,
            packet.data,
            encryption_monitor,
            fingerprint_monitor
        )
        .await
    );

    send_up(
        Ok(ManagerMessage::Packet(ready_packet.wrap(packet.src_addr))),
        sender.clone(),
    )
    .await;
}

async fn deserialize_and_decrypt(
    packet_type: PacketType,
    mut data: Vec<u8>,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) -> core::result::Result<Packet, ()> {
    let session_id = SessionId::deserialize(&data[SESSION_ID_OFFSET..])?;

    match packet_type {
        // Single types
        PacketType::Data => {
            let mut packet = Box::new(DataPacket::deserialize(&data)?);
            encryption::decrypt(packet.as_mut(), session_id, encryption_monitor)?;
            Ok(Packet::DataPacket(packet))
        }

        PacketType::Parity => {
            let mut packet = Box::new(ParityPacket::deserialize(&data)?);
            encryption::decrypt(packet.as_mut(), session_id, encryption_monitor)?;
            Ok(Packet::ParityPacket(packet))
        }

        PacketType::Ack => {
            authenticate(&mut data, session_id, encryption_monitor)?;
            let data = dedup_no_payload(data, session_id, fingerprint_monitor)
                .await
                .ok_or(())?;
            let packet = Packet::AckPacket(Box::new(AckPacket::deserialize(&data)?));

            Ok(packet)
        }

        // Subtypes
        PacketType::Host | PacketType::Session | PacketType::Playback => {
            deserialize_and_auth_control_packet(
                data,
                session_id,
                encryption_monitor,
                fingerprint_monitor,
            )
            .await
        }
        PacketType::Error => {
            deserialize_and_auth_error_packet(
                data,
                session_id,
                encryption_monitor,
                fingerprint_monitor,
            )
            .await
        }

        // Not yet
        PacketType::Metadata => unimplemented!(),
    }
}

async fn deserialize_and_auth_control_packet(
    mut data: Vec<u8>,
    session_id: SessionId,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) -> core::result::Result<Packet, ()> {
    Ok(
        match ControlType::deserialize(&data[SECONDARY_TYPE_OFFSET..])? {
            // host
            ControlType::Host(HostControlType::Hello) => {
                authenticate(&mut data, session_id, encryption_monitor)?;
                let data = dedup_no_payload(data, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::HelloPacket(Box::new(HelloPacket::deserialize(&data)?))
            }

            //session
            ControlType::Session(SessionControlType::Retransmit) => {
                let packet = Box::new(RetransmitPacket::deserialize(&data)?);
                authenticate(&mut packet.headers(), session_id, encryption_monitor)?;
                let packet = dedup_with_payload(packet, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::RetransmitPacket(packet)
            }
            ControlType::Session(SessionControlType::TrackRequest) => {
                let mut packet = Box::new(TrackRequestPacket::deserialize(&data)?);
                encryption::decrypt(packet.as_mut(), session_id, encryption_monitor)?;
                let packet = dedup_with_payload(packet, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::TrackRequestPacket(packet)
            }
            ControlType::Session(SessionControlType::MetadataRequest) => unimplemented!(),

            //playback
            ControlType::Playback(_) => {
                authenticate(&mut data, session_id, encryption_monitor)?;
                let data = dedup_no_payload(data, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::PlaybackStatusPacket(Box::new(PlaybackStatusPacket::deserialize(&data)?))
            }
        },
    )
}

async fn deserialize_and_auth_error_packet(
    mut data: Vec<u8>,
    session_id: SessionId,
    encryption_monitor: &'static EncryptionMonitor<'_>,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) -> core::result::Result<Packet, ()> {
    Ok(
        match ErrorType::deserialize(&data[SECONDARY_TYPE_OFFSET..])? {
            ErrorType::AppReject => {
                let mut packet = Box::new(AppRejectErrorPacket::deserialize(&data)?);
                encryption::decrypt(packet.as_mut(), session_id, encryption_monitor)?;
                let packet = dedup_with_payload(packet, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::AppRejectErrorPacket(packet)
            }

            ErrorType::UnexpectedPacket | ErrorType::IncomprehensiblePacket => {
                authenticate(&mut data, session_id, encryption_monitor)?;
                let data = dedup_no_payload(data, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::UnexpectedPacketErrorPacket(Box::new(
                    UnexpectedPacketErrorPacket::deserialize(&data)?,
                ))
            }

            ErrorType::SessionDoesNotExist => {
                authenticate(&mut data, session_id, encryption_monitor)?;
                let data = dedup_no_payload(data, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::SessionDoesNotExistErrorPacket(Box::new(
                    SessionDoesNotExistErrorPacket::deserialize(&data)?,
                ))
            }
        },
    )
}

fn authenticate(
    packet: &mut Vec<u8>,
    session_id: SessionId,
    encryption_monitor: &'static EncryptionMonitor<'_>,
) -> core::result::Result<(), ()> {
    if encryption::authenticate(packet, session_id, encryption_monitor) {
        Ok(())
    } else {
        Err(())
    }
}

async fn dedup_no_payload(
    packet: Vec<u8>,
    session_id: SessionId,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) -> Option<Vec<u8>> {
    let fingerprint = Box::new((&packet).into());
    let window = fingerprint_monitor.get(&session_id).await;
    if window.contains(&fingerprint).await {
        None
    } else {
        _ = window.add(fingerprint).await;
        Some(packet)
    }
}

async fn dedup_with_payload<T: Headers>(
    packet: Box<T>,
    session_id: SessionId,
    fingerprint_monitor: &'static FingerprintMonitor<'_>,
) -> Option<Box<T>> {
    let window = fingerprint_monitor.get(&session_id).await;
    let fingerprint: Box<PacketFingerprint> = Box::new(packet.as_ref().into());
    if window.contains(&fingerprint).await {
        None
    } else {
        window.add(fingerprint).await;
        Some(packet)
    }
}

async fn send_up(message: Result<ManagerMessage>, sender: InboundSender) {
    _ = sender.send(message).await;
}
