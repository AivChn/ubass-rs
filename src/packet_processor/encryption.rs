use crate::manager::types::EncryptionMonitor;
use crate::packetizer::fingerprint::Headers;
use crate::packetizer::types::{DataPacket, ParityPacket, SessionId};
use crate::prelude::*;

use aes_gcm_siv::{AeadInPlace, Aes256GcmSiv, Nonce};

pub enum Encryptable {
    Data(DataPacket),
    Parity(ParityPacket),
}

fn get_nonce(session_id: SessionId, counter: [u8; 8]) -> [u8; 12] {
    let mut result = [0u8; 12];
    let session_part = &session_id.0.to_be_bytes()[..4];
    result[..4].copy_from_slice(session_part);
    result[4..].copy_from_slice(&counter);
    result
}

pub fn encrypt(packet: &mut Encryptable, monitor: &EncryptionMonitor) {
    let (aad, payload, session_id) = match packet {
        Encryptable::Data(packet) => (packet.headers(), &mut packet.payload, packet.session_id),
        Encryptable::Parity(packet) => (packet.headers(), &mut packet.payload, packet.session_id),
    };

    let (cipher, counter) = monitor.get(&session_id);
    let nonce = Nonce::from(get_nonce(session_id, counter));

    cipher.encrypt_in_place(&nonce, &aad, payload);

    payload.extend(counter);
}

pub fn decrypt(packet: &mut Encryptable, monitor: &EncryptionMonitor) -> bool {
    let (aad, payload, session_id) = match packet {
        Encryptable::Data(packet) => (packet.headers(), &mut packet.payload, packet.session_id),
        Encryptable::Parity(packet) => (packet.headers(), &mut packet.payload, packet.session_id),
    };

    let cipher = monitor.get_cipher(&session_id);
    let counter: [u8; 8] = payload
        .drain(payload.len() - 8..)
        .collect::<Vec<_>>()
        .try_into()
        .expect("length is guaranteed");
    let nonce = Nonce::from(get_nonce(session_id, counter));

    cipher.decrypt_in_place(&nonce, &aad, payload).is_ok()
}

pub fn tag(packet: &mut Vec<u8>, session_id: SessionId, monitor: &EncryptionMonitor) {
    let (cipher, counter) = monitor.get(&session_id);
    let nonce = Nonce::from(get_nonce(session_id, counter));

    let mut tag = vec![];

    cipher.encrypt_in_place(&nonce, packet, &mut tag);
    packet.append(&mut tag);
    packet.extend(counter);
}

pub fn authenticate(packet: &mut Vec<u8>, session_id: SessionId, monitor: &EncryptionMonitor) {
    let cipher = monitor.get_cipher(&session_id);
    let counter: [u8; 8] = packet
        .drain(packet.len() - 8..)
        .collect::<Vec<_>>()
        .try_into()
        .expect("length is guaranteed");
    let nonce = Nonce::from(get_nonce(session_id, counter));

    let mut tag: Vec<u8> = packet.drain(packet.len() - 16..).collect();

    cipher.decrypt_in_place(&nonce, packet, &mut tag);
}
