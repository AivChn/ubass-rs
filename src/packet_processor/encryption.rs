use super::fingerprint::{Headers, Payload};
use crate::manager::EncryptionMonitor;
use crate::manager::packets::{
    AppRejectErrorPacket, DataPacket, ParityPacket, SessionId, TrackRequestPacket,
};
use crate::prelude::*;

use aes_gcm_siv::{AeadInPlace, Nonce};

pub trait Encryptable: Payload + Headers {}

impl Encryptable for DataPacket {}
impl Encryptable for ParityPacket {}
impl Encryptable for TrackRequestPacket {}
impl Encryptable for AppRejectErrorPacket {}

fn get_nonce(session_id: SessionId, counter: [u8; 8]) -> [u8; 12] {
    let mut result = [0u8; 12];
    let session_part = &session_id.to_be_bytes()[..4];
    result[..4].copy_from_slice(session_part);
    result[4..].copy_from_slice(&counter);
    result
}

pub async fn encrypt(
    packet: &mut impl Encryptable,
    session_id: SessionId,
    monitor: EncryptionMonitor,
) {
    let (aad, payload) = (packet.headers(), packet.payload());

    let (cipher, counter) = monitor.get(&session_id).await;
    let nonce = Nonce::from(get_nonce(session_id, counter));

    _ = cipher.encrypt_in_place(&nonce, &aad, payload);

    payload.extend(counter);
}

/// Decrypts the buffer in place, while authenticating the data at the same time.
/// This function will remove the nonce and authenticatation tag from the payload, shortening it by
/// 24 bytes no matter what.
///
/// # Errors
/// This function returns Err(()) if decryption or authenticatation failed. If this value is
/// returned, the protocol considers the packet unuseable.
///
/// # Panics
/// This function panics if an 8 byte slice could not be converted to an 8 byte array. that is, never.
pub async fn decrypt(
    packet: &mut impl Encryptable,
    session_id: SessionId,
    monitor: EncryptionMonitor,
) -> EmptyResult {
    let (aad, payload) = (packet.headers(), packet.payload());

    // len < minimum payload + tag + nonce counter
    if payload.len() < 1 + 16 + 8 {
        return Err(());
    }

    let cipher = monitor.get_cipher(&session_id).await;
    let counter: [u8; 8] = payload[payload.len() - 8..]
        .try_into()
        .expect("failed to convert an 8 byte slice to an array of 8 bytes.");
    payload.truncate(payload.len() - 8);
    let nonce = Nonce::from(get_nonce(session_id, counter));

    cipher
        .decrypt_in_place(&nonce, &aad, payload)
        .map_err(|_| ())
}

pub async fn tag(packet: &mut Vec<u8>, session_id: SessionId, monitor: EncryptionMonitor) {
    let (cipher, counter) = monitor.get(&session_id).await;
    let nonce = Nonce::from(get_nonce(session_id, counter));

    let mut tag = vec![];

    _ = cipher.encrypt_in_place(&nonce, packet, &mut tag);
    packet.append(&mut tag);
    packet.extend(counter);
}

/// authenticates the given buffer by passing it as aad to decryption with a buffer of just the tag.
/// This function assumes the data comes in the shape of [data | tag | nonce counter].
/// It will return true on successes, that is if the authentication was successfull it will return
/// true, if authentication ailed it will return false. either way at least the nonce counter will
/// be consumed.
/// A packet that failed authentication is considered unuseable by the protocol.
///
/// # Panics
/// This function panics if an 8 byte slice could not be converted to an 8 byte array. that is, never.
pub async fn authenticate(
    packet: &mut Vec<u8>,
    session_id: SessionId,
    monitor: EncryptionMonitor,
) -> bool {
    let cipher = monitor.get_cipher(&session_id).await;
    let counter: [u8; 8] = packet
        .drain(packet.len() - 8..)
        .collect::<Vec<_>>()
        .try_into()
        .expect("Failed to convert an 8 byte slice to an 8 byte array");
    let nonce = Nonce::from(get_nonce(session_id, counter));

    let mut tag: Vec<u8> = packet.drain(packet.len() - 16..).collect();

    cipher.decrypt_in_place(&nonce, packet, &mut tag).is_ok()
}
