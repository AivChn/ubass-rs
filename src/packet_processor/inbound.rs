#![allow(clippy::wildcard_imports)]

use crate::{
    manager::{EncryptionMonitor, FingerprintMonitor, packets::*},
    packet_processor::{encryption, fingerprint::Headers, serialize::Serialize},
    prelude::*,
    r_unwrap_or_return,
    transport::types::ReceivedPacket,
};

use super::types::{InboundChannels, InboundSender};

const SESSION_ID_OFFSET: usize = 8;
const PACKET_TYPE_OFFSET: usize = 4;
const SECONDARY_TYPE_OFFSET: usize = 5;

pub async fn init(
    InboundChannels {
        mut t_receiver,
        p_sender,
    }: InboundChannels,
    encryption_monitor: EncryptionMonitor,
    fingerprint_monitor: FingerprintMonitor,
) -> ErrResult {
    loop {
        let received = t_receiver.recv().await;
        let Some(message) = received else {
            return Err(ChannelError::ChannelClosed(Inbound).into());
        };

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
                tokio::spawn(send_up(Err(err), p_sender.clone()));
                continue;
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

async fn handle_packet(
    packet: ReceivedPacket,
    sender: InboundSender,
    encryption_monitor: EncryptionMonitor,
    fingerprint_monitor: FingerprintMonitor,
) {
    let version = r_unwrap_or_return!(Version::deserialize(&packet.data));

    if version.is_zero() {
        let ready_packet =
            r_unwrap_or_return!(IncompatibleVersionPacket::deserialize(&packet.data));
        tokio::spawn(send_up(
            Ok(ManagerMessage::Packet(
                Packet::IncompatibleVersionPacket(Box::new(ready_packet)).wrap(packet.src_addr),
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
        r_unwrap_or_return!(PacketType::deserialize(&packet.data[PACKET_TYPE_OFFSET..]));

    let ready_packet = r_unwrap_or_return!(
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
    data: Vec<u8>,
    encryption_monitor: EncryptionMonitor,
    fingerprint_monitor: FingerprintMonitor,
) -> core::result::Result<Packet, ()> {
    let session_id = SessionId::deserialize(&data[SESSION_ID_OFFSET..])?;

    match packet_type {
        // Single types
        PacketType::Data => {
            let mut packet = Box::new(DataPacket::deserialize(&data)?);
            let session_id = packet.session_id;
            encryption::decrypt(packet.as_mut(), session_id, encryption_monitor).await?;
            Ok(Packet::DataPacket(packet))
        }

        PacketType::Parity => {
            let mut packet = Box::new(ParityPacket::deserialize(&data)?);
            let session_id = packet.session_id;
            encryption::decrypt(packet.as_mut(), session_id, encryption_monitor).await?;
            Ok(Packet::ParityPacket(packet))
        }

        PacketType::Ack => {
            // AckPackets complete the handshake — the session cipher and fingerprint table may
            // not be established yet when they arrive, so authentication is deferred to the manager.
            let packet = Packet::AckPacket(Box::new(AckPacket::deserialize(&data)?));
            Ok(packet)
        }

        PacketType::HandshakeAck => Ok(Packet::HandshakeAckPacket(Box::new(
            HandshakeAckPacket::deserialize(&data)?,
        ))),

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
    encryption_monitor: EncryptionMonitor,
    fingerprint_monitor: FingerprintMonitor,
) -> core::result::Result<Packet, ()> {
    Ok(
        match ControlType::deserialize(&data[SECONDARY_TYPE_OFFSET..])? {
            // host
            ControlType::Host(HostControlType::Hello) => {
                // HelloPackets are pre-session — no shared key exists yet so they cannot
                // be authenticated or deduplicated here. Authentication is handled at the
                // app level via the approval mechanism.
                Packet::HelloPacket(Box::new(HelloPacket::deserialize(&data)?))
            }

            //session
            ControlType::Session(SessionControlType::Retransmit) => {
                let packet = Box::new(RetransmitPacket::deserialize(&data)?);
                authenticate(&mut packet.headers(), session_id, encryption_monitor).await?;
                let packet = dedup_with_payload(packet, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::RetransmitPacket(packet)
            }
            ControlType::Session(SessionControlType::TrackRequest) => {
                let mut packet = Box::new(TrackRequestPacket::deserialize(&data)?);
                encryption::decrypt(packet.as_mut(), session_id, encryption_monitor).await?;
                let packet = dedup_with_payload(packet, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::TrackRequestPacket(packet)
            }
            ControlType::Session(SessionControlType::MetadataRequest) => unimplemented!(),

            //playback
            ControlType::Playback(_) => {
                authenticate(&mut data, session_id, encryption_monitor).await?;
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
    encryption_monitor: EncryptionMonitor,
    fingerprint_monitor: FingerprintMonitor,
) -> core::result::Result<Packet, ()> {
    Ok(
        match ErrorType::deserialize(&data[SECONDARY_TYPE_OFFSET..])? {
            ErrorType::AppReject => {
                let mut packet = Box::new(AppRejectErrorPacket::deserialize(&data)?);
                encryption::decrypt(packet.as_mut(), session_id, encryption_monitor).await?;
                let packet = dedup_with_payload(packet, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::AppRejectErrorPacket(packet)
            }

            ErrorType::UnexpectedPacket | ErrorType::IncomprehensiblePacket => {
                authenticate(&mut data, session_id, encryption_monitor).await?;
                let data = dedup_no_payload(data, session_id, fingerprint_monitor)
                    .await
                    .ok_or(())?;
                Packet::UnexpectedPacketErrorPacket(Box::new(dbg!(
                    UnexpectedPacketErrorPacket::deserialize(&data)?
                )))
            }

            ErrorType::SessionDoesNotExist => {
                authenticate(&mut data, session_id, encryption_monitor).await?;
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

async fn authenticate(
    packet: &mut Vec<u8>,
    session_id: SessionId,
    encryption_monitor: EncryptionMonitor,
) -> core::result::Result<(), ()> {
    if encryption::authenticate(packet, session_id, encryption_monitor).await {
        Ok(())
    } else {
        Err(())
    }
}

async fn dedup_no_payload(
    packet: Vec<u8>,
    session_id: SessionId,
    fingerprint_monitor: FingerprintMonitor,
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
    fingerprint_monitor: FingerprintMonitor,
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

#[cfg(test)]
mod test {
    use std::sync::{Arc, LazyLock, atomic::AtomicU64};

    use tokio::time::Instant;

    use crate::{
        lock_write,
        manager::{
            packets::{
                BatchID, BytePosition, DataPacket, FECInfo, Options, PacketFingerprint, SessionId,
            },
            state::*,
        },
        packet_processor::{
            fingerprint,
            inbound::{dedup_no_payload, dedup_with_payload},
        },
        prelude::{PROTOCOL_EPOCH, Timestamp},
        utils::Flags,
    };

    static FINGERPRINTS: LazyLock<FingerprintTable> = LazyLock::new(FingerprintTable::default);

    static SESSION_ID: AtomicU64 = AtomicU64::new(1);

    fn next_session() -> SessionId {
        _ = PROTOCOL_EPOCH.get_or_init(Instant::now);
        SessionId::new(SESSION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }

    #[tokio::test]
    async fn dedup_without_payload_not_duplicate() {
        let session_id = next_session();
        let packet = b"random packet".to_vec();
        FINGERPRINTS
            .write()
            .await
            .insert(session_id, Arc::default());
        let fingerprint_monitor = FingerprintMonitor::new(&FINGERPRINTS);
        let some = dedup_no_payload(packet.clone(), session_id, fingerprint_monitor).await;
        assert_eq!(some, Some(packet));
    }

    #[tokio::test]
    async fn dedup_without_payload_duplicate() {
        let session_id = next_session();
        let mut packet = b"random packet".to_vec();
        packet.extend_from_slice(&session_id.to_be_bytes());
        FINGERPRINTS
            .write()
            .await
            .insert(session_id, Arc::default());
        let fingerprint_monitor = FingerprintMonitor::new(&FINGERPRINTS);
        let fingerprint = Box::new((&packet).into());
        assert!(
            fingerprint_monitor
                .get(&session_id)
                .await
                .add(fingerprint)
                .await
        );
        let none = dedup_no_payload(packet.clone(), session_id, fingerprint_monitor).await;
        dbg!(&none);
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn dedup_with_payload_duplicate() {
        let session_id = next_session();
        let mut packet = DataPacket::new(
            Options::none(),
            BatchID::new(2),
            FECInfo {
                batch_size: 10,
                batch_pos: 2,
                recovery_count: 3,
            },
            next_session(),
            BytePosition(12),
            vec![1, 2, 3, 4, 5],
        );
        packet.payload.extend_from_slice(&session_id.to_be_bytes());
        FINGERPRINTS
            .write()
            .await
            .insert(session_id, Arc::default());
        let fingerprint_monitor = FingerprintMonitor::new(&FINGERPRINTS);
        let fingerprint = Box::new(PacketFingerprint::from(&*packet));
        assert!(
            fingerprint_monitor
                .get(&session_id)
                .await
                .add(fingerprint)
                .await
        );
        //let none = dedup_no_payload(packet.clone(), session_id, fingerprint_monitor).await;
        let none = dedup_with_payload(packet, session_id, fingerprint_monitor).await;
        dbg!(&none);
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn dedup_with_payload_not_duplicate() {
        let session_id = next_session();
        let mut packet = DataPacket::new(
            Options::none(),
            BatchID::new(2),
            FECInfo {
                batch_size: 10,
                batch_pos: 2,
                recovery_count: 3,
            },
            next_session(),
            BytePosition(12),
            vec![1, 2, 3, 4, 5],
        );
        packet.payload.extend_from_slice(&session_id.to_be_bytes());
        FINGERPRINTS
            .write()
            .await
            .insert(session_id, Arc::default());
        let fingerprint_monitor = FingerprintMonitor::new(&FINGERPRINTS);
        let some = dedup_with_payload(packet, session_id, fingerprint_monitor).await;
        dbg!(&some);
        assert!(some.is_some());
    }
}
