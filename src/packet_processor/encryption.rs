use crate::manager::packets::fingerprint::{Headers, Payload};
use crate::manager::packets::types::{
    AppRejectErrorPacket, DataPacket, ParityPacket, SessionId, TrackRequestPacket,
};
use crate::manager::types::EncryptionMonitor;
use crate::prelude::*;

use aes_gcm_siv::{AeadInPlace, Nonce};

pub trait Encryptable: Payload + Headers {}

impl Encryptable for DataPacket {}
impl Encryptable for ParityPacket {}
impl Encryptable for TrackRequestPacket {}
impl Encryptable for AppRejectErrorPacket {}

fn get_nonce(session_id: SessionId, counter: [u8; 8]) -> [u8; 12] {
    let mut result = [0u8; 12];
    let session_part = &session_id.0.to_be_bytes()[..4];
    result[..4].copy_from_slice(session_part);
    result[4..].copy_from_slice(&counter);
    result
}

pub fn encrypt(packet: &mut impl Encryptable, session_id: SessionId, monitor: &EncryptionMonitor) {
    let (aad, payload) = (packet.headers(), packet.payload());

    let (cipher, counter) = monitor.get(&session_id);
    let nonce = Nonce::from(get_nonce(session_id, counter));

    _ = cipher.encrypt_in_place(&nonce, &aad, payload);

    payload.extend(counter);
}

pub fn decrypt(
    packet: &mut impl Encryptable,
    session_id: SessionId,
    monitor: &EncryptionMonitor,
) -> EmptyResult {
    let (aad, payload) = (packet.headers(), packet.payload());

    // len < minimum payload + tag + nonce counter
    if payload.len() < 1 + 16 + 8 {
        return Err(());
    }

    let cipher = monitor.get_cipher(&session_id);
    let counter: [u8; 8] = payload[payload.len() - 8..]
        .try_into()
        .expect("length is guaranteed");
    payload.truncate(payload.len() - 8);
    let nonce = Nonce::from(get_nonce(session_id, counter));

    cipher
        .decrypt_in_place(&nonce, &aad, payload)
        .map_err(|_| ())
}

pub fn tag(packet: &mut Vec<u8>, session_id: SessionId, monitor: &EncryptionMonitor) {
    let (cipher, counter) = monitor.get(&session_id);
    let nonce = Nonce::from(get_nonce(session_id, counter));

    let mut tag = vec![];

    _ = cipher.encrypt_in_place(&nonce, packet, &mut tag);
    packet.append(&mut tag);
    packet.extend(counter);
}

pub fn authenticate(
    packet: &mut Vec<u8>,
    session_id: SessionId,
    monitor: &EncryptionMonitor,
) -> bool {
    let cipher = monitor.get_cipher(&session_id);
    let counter: [u8; 8] = packet[packet.len() - 8..]
        .try_into()
        .expect("length is guaranteed");
    let nonce = Nonce::from(get_nonce(session_id, counter));

    let mut tag: Vec<u8> = packet.drain(packet.len() - 16..).collect();

    cipher.decrypt_in_place(&nonce, packet, &mut tag).is_ok()
}
