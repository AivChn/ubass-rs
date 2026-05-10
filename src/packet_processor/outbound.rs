#![allow(clippy::pedantic)]
use std::{
    net::SocketAddr,
    ptr::dangling,
    sync::{Arc, atomic::Ordering},
};

use tokio::sync::oneshot;
use tracing::{error, instrument};

use crate::{
    manager::{
        EncryptionMonitor, PendingAckMonitor,
        packets::{
            BatchID, OptionFlags, Packet, PacketFingerprint, PacketType, PacketWrapper,
            ParityPacket, SessionId,
        },
        state,
    },
    packet_processor::{
        encryption::{self, Encryptable},
        fec::{self, FECCompatible, Recovered},
        serialize::{self, Serialize},
        types::{InboundSender, OutboundSender, ProcessedPacket},
    },
    prelude::*,
    r_unwrap_or_return,
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
                $pending_ack_monitor,
            )
        }
    };
}

pub async fn init(
    OutboundChannels {
        to_transport: t_sender,
        to_manager: p_sender,
        from_manager: mut p_receiver,
    }: OutboundChannels,
    encryption_monitor: EncryptionMonitor,
    pending_ack_monitor: PendingAckMonitor,
) -> ErrResult {
    // initialize handle monitor
    let monitor = Arc::from(HandleMonitor::default());

    loop {
        let received = p_receiver.recv().await;
        let msg = received.ok_or(ChannelError::ChannelClosed(
            Outbound,
            Layer::PacketProcessor,
        ))?;

        let packet = match msg {
            PacketProcessingMessage::SendPacket(packet_wrapper) => packet_wrapper,
            PacketProcessingMessage::Recover(OneShot {
                data: (session_id, batch_id),
                response,
            }) => {
                monitor.dispatch(recover(session_id, batch_id, response));
                continue;
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
        };

        monitor.dispatch(handle_packet(
            packet,
            p_sender.clone(),
            t_sender.clone(),
            encryption_monitor,
            monitor.clone(),
            pending_ack_monitor,
        ));
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_packet(
    packet: PacketWrapper,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    encryption_monitor: EncryptionMonitor,
    handle_monitor: Arc<HandleMonitor>,
    pending_ack_monitor: PendingAckMonitor,
) {
    let addr = packet.addr;
    let processed = match packet.packet {
        // fec + encrypted
        // could be acked
        Packet::DataPacket(mut packet) => {
            add_ack!(for DataPacket(packet), sent to addr, saved to pending_ack_monitor);

            handle_monitor.dispatch(handle_fec(
                *packet.clone(),
                addr,
                p_sender.clone(),
                t_sender.clone(),
                encryption_monitor,
                handle_monitor.clone(),
            ));

            let session_id = packet.session_id;
            encryption::encrypt(packet.as_mut(), session_id, encryption_monitor).await;

            let mut serialized;
            serialize!(packet -> serialized);
            processed!(serialized to addr as Data 1 times)
        }

        //encrypted
        Packet::TrackRequestPacket(packet) => {
            add_ack!(for TrackRequestPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            let serialized = process_encrypted(packet, session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Session 3 times)
        }
        Packet::AppRejectErrorPacket(packet) => {
            add_ack!(for AppRejectErrorPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            let serialized = process_encrypted(packet, session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Error 3 times)
        }

        // authenticated
        Packet::RetransmitPacket(packet) => {
            add_ack!(for RetransmitPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            let serialized =
                process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Session 3 times)
        }
        Packet::PlaybackControlPacket(packet) => {
            add_ack!(for PlaybackControlPacket(packet), sent to addr, saved to pending_ack_monitor);

            let session_id = packet.session_id;
            let serialized =
                process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Playback 3 times)
        }
        Packet::CloseSessionPacket(packet) => {
            let session_id = packet.session_id;
            let serialized =
                process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Session 3 times)
        }
        Packet::SessionDoesNotExistErrorPacket(packet) => {
            add_ack!(
                for SessionDoesNotExistErrorPacket(packet),
                sent to addr,
                saved to pending_ack_monitor
            );

            let session_id = packet.session_id;
            let serialized =
                process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Error 3 times)
        }
        Packet::KeepAlivePacket(packet) => {
            let session_id = packet.session_id;
            let mut serialized;
            serialize!(packet -> serialized);

            encryption::tag(&mut serialized, session_id, encryption_monitor).await;
            ProcessedPacket {
                dest_addr: addr,
                packet_type: PacketType::KeepAlive,
                data: serialized,
                duplicate_count: 3,
            }
        }
        // could not be acked
        Packet::UnexpectedPacketErrorPacket(packet) => {
            let session_id = packet.session_id;
            let serialized =
                process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Error 3 times)
        }
        Packet::AckPacket(packet) => {
            let session_id = packet.session_id;
            let serialized =
                process_authenticated(packet.as_ref(), session_id, addr, encryption_monitor).await;
            processed!(serialized to addr as Ack 3 times)
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
        Packet::HandshakeRejection(packet) => {
            let mut serialized;
            serialize!(packet -> serialized);

            ProcessedPacket {
                dest_addr: addr,
                packet_type: PacketType::Host,
                data: serialized,
                duplicate_count: 3,
            }
        }
        Packet::HandshakeAckPacket(packet) => {
            let mut serialized;
            serialize!(packet -> serialized);

            ProcessedPacket {
                dest_addr: addr,
                packet_type: PacketType::HandshakeAck,
                data: serialized,
                duplicate_count: 3,
            }
        }
        Packet::IncompatibleVersionPacket(packet) => {
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

async fn process_encrypted(
    mut packet: Box<impl Encryptable + Serialize>,
    session_id: SessionId,
    addr: SocketAddr,
    encryption_monitor: EncryptionMonitor,
) -> Vec<u8> {
    encryption::encrypt(packet.as_mut(), session_id, encryption_monitor).await;

    let mut serialized;
    serialize!(packet -> serialized);
    serialized
}

async fn process_authenticated(
    packet: &impl Serialize,
    session_id: SessionId,
    addr: SocketAddr,
    encryption_monitor: EncryptionMonitor,
) -> Vec<u8> {
    let mut serialized;
    serialize!(packet -> serialized);

    encryption::tag(&mut serialized, session_id, encryption_monitor).await;
    serialized
}

async fn recover(
    session_id: SessionId,
    batch_id: BatchID,
    sender: oneshot::Sender<core::result::Result<Recovered, CouldNotRecover>>,
) {
    let message = match fec::recover(batch_id, session_id).await {
        Some(recovered) => Ok(recovered),
        None => Err(CouldNotRecover),
    };

    _ = sender.send(message);
}

fn add_pending_ack(packet: PacketWrapper, pending_ack_monitor: PendingAckMonitor) {
    pending_ack_monitor.add(packet.packet);
}

async fn handle_fec(
    packet: impl FECCompatible,
    dest_addr: SocketAddr,
    p_sender: InboundSender,
    t_sender: OutboundSender,
    encryption_monitor: EncryptionMonitor,
    handle_monitor: Arc<HandleMonitor>,
) {
    if let Some(parity_packets) = fec::sent(packet).await {
        for packet in parity_packets {
            handle_monitor.dispatch(handle_parity(
                packet,
                dest_addr,
                t_sender.clone(),
                p_sender.clone(),
                encryption_monitor,
            ));
        }
    }
}

#[allow(clippy::unused_async)] // async is for spawning the function
async fn handle_parity(
    mut packet: ParityPacket,
    dest_addr: SocketAddr,
    t_sender: OutboundSender,
    p_sender: InboundSender,
    encryption_monitor: EncryptionMonitor,
) {
    let mut buffer;
    let session_id = packet.session_id;
    encryption::encrypt(&mut packet, session_id, encryption_monitor).await;
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

#[instrument(skip_all)]
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
        error!("Channel closed!");
        send_to_manager(
            Err(ChannelError::ChannelFailed(Outbound, Layer::PacketProcessor).into()),
            p_sender,
        )
        .await;
    }
}

#[cfg(test)]
mod test_packet_processor_macros {
    use std::net::{IpAddr, Ipv4Addr};

    use crate::manager::packets::{BytePosition, DataPacket, Options, Version};

    use super::*;

    fn get_data_packet() -> DataPacket {
        packets::DataPacket {
            version: Version::CURRENT_VERSION,
            opts: Options::none(),
            packet_type: packets::PacketType::Data,
            batch_id: BatchID::new(9),
            fec_info: packets::FECInfo {
                batch_size: 9,
                batch_pos: 2,
                recovery_count: 5,
            },
            session_id: SessionId::new(5),
            timestamp: Timestamp(120),
            byte_range_start: BytePosition(8),
            payload: vec![1u8, 2, 3, 4, 5].into(),
        }
    }

    const SERIALIZED_DATA_PACKET: [u8; 35] = [
        /*version*/ 0,
        1,
        /*opts*/ 0,
        0,
        PacketType::Data as u8,
        /*batch_id*/ 0,
        9,
        /*fec_info*/ 9,
        2,
        5,
        /*session_id*/ 0,
        0,
        0,
        0,
        0,
        0,
        0,
        5,
        /*timestamp*/ 0,
        0,
        0,
        0,
        0,
        0,
        0,
        120,
        /*byte_position*/ 0,
        0,
        0,
        8,
        /*payload*/ 1,
        2,
        3,
        4,
        5,
    ];

    #[test]
    fn test_serialize() {
        let packet = get_data_packet();
        let mut buf;
        serialize!(packet -> buf);
        assert_eq!(&buf, &SERIALIZED_DATA_PACKET);
    }

    #[test]
    fn test_processed() {
        let packet = get_data_packet();
        let mut buf;
        serialize!(packet -> buf);
        assert_eq!(&buf, &SERIALIZED_DATA_PACKET);

        let cloned = buf.clone();
        let address = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let processed = processed!(buf to address as Data 1 times);
        let correct = ProcessedPacket {
            dest_addr: address,
            packet_type: PacketType::Data,
            data: cloned,
            duplicate_count: 1,
        };

        assert_eq!(processed, correct);
    }
}

#[cfg(test)]
mod integration_tests {
    use core::panic;
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{LazyLock, OnceLock},
        time::Duration,
    };

    use tokio::sync::mpsc::{Receiver, Sender};

    use crate::{
        manager::{
            EncryptionMonitor, PendingAckMonitor,
            state::{EncryptionTable, PendingAckWindow},
        },
        packet_processor::types::OutboundChannels,
        transport::{self, types::ReceivedPacket},
        utils::{ManagerMessage, PacketProcessingMessage, TransportMessage},
    };

    static ENCRYPTION: LazyLock<EncryptionTable> = LazyLock::new(EncryptionTable::default);
    static PENDING_ACK: OnceLock<PendingAckWindow> = OnceLock::new();

    type TestInitTypes = (
        (
            Sender<PacketProcessingMessage>,
            Receiver<PacketProcessingMessage>,
        ),
        (
            Sender<crate::prelude::Result<ManagerMessage>>,
            Receiver<crate::prelude::Result<ManagerMessage>>,
        ),
        (Sender<TransportMessage>, Receiver<TransportMessage>),
        (EncryptionMonitor, PendingAckMonitor),
    );

    fn prep_for_init() -> TestInitTypes {
        let (processor_to_transport, transport_from_processor) = tokio::sync::mpsc::channel(1);
        (
            (processor_to_transport.clone(), transport_from_processor),
            tokio::sync::mpsc::channel(1),
            tokio::sync::mpsc::channel(1),
            (
                EncryptionMonitor::new(&ENCRYPTION),
                PendingAckMonitor::new(
                    PENDING_ACK.get_or_init(|| PendingAckWindow::new(processor_to_transport)),
                ),
            ),
        )
    }

    #[tokio::test]
    async fn outbound_panics_on_closed_message() {
        let (
            (t_sender, t_receiver),
            (dud_sender, _),
            (dud2_sender, _),
            (encryption_monitor, pending_ack_monitor),
        ) = prep_for_init();

        let handle = tokio::spawn(super::init(
            OutboundChannels {
                to_transport: dud2_sender.clone(),
                to_manager: dud_sender.clone(),
                from_manager: t_receiver,
            },
            encryption_monitor,
            pending_ack_monitor,
        ));

        _ = t_sender.send(PacketProcessingMessage::Closed).await;
        assert!(matches!(handle.await, Err(e) if e.is_panic()));
    }

    #[tokio::test]
    async fn outbound_panics_on_received_message() {
        let (
            (t_sender, t_receiver),
            (dud_sender, _),
            (dud2_sender, _),
            (encryption_monitor, pending_ack_monitor),
        ) = prep_for_init();

        let handle = tokio::spawn(super::init(
            OutboundChannels {
                to_transport: dud2_sender,
                to_manager: dud_sender,
                from_manager: t_receiver,
            },
            encryption_monitor,
            pending_ack_monitor,
        ));

        _ = t_sender
            .send(PacketProcessingMessage::ReceivedPacket(ReceivedPacket {
                src_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 8),
                data: vec![0u8],
            }))
            .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(matches!(handle.await, Err(e) if e.is_panic()));
    }

    #[tokio::test]
    async fn returned_ok_on_close() {
        let (
            (t_sender, t_receiver),
            (dud_sender, _),
            (dud2_sender, _),
            (encryption_monitor, pending_ack_monitor),
        ) = prep_for_init();

        let handle = tokio::spawn(super::init(
            OutboundChannels {
                to_transport: dud2_sender.clone(),
                to_manager: dud_sender.clone(),
                from_manager: t_receiver,
            },
            encryption_monitor,
            pending_ack_monitor,
        ));

        _ = t_sender.send(PacketProcessingMessage::Close).await;
        assert!(matches!(handle.await, Ok(Ok(()))));
    }
}
