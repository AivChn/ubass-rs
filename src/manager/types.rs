use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use aes_gcm_siv::Aes256GcmSiv;

use crate::packetizer::types::SessionId;

pub enum ManagerMessage {
    Close,
    Highway,
}

#[derive(PartialEq)]
pub enum HighwayMessage {
    Packetizer,
    PacketProcessor,
    Transport,
}

pub struct EncryptionWindow {
    cipher: Aes256GcmSiv,
    nonce: AtomicU64,
}

impl EncryptionWindow {
    pub fn new(cipher: Aes256GcmSiv) -> Self {
        Self {
            cipher,
            nonce: AtomicU64::new(0),
        }
    }

    pub fn get(&self) -> (&Aes256GcmSiv, [u8; 8]) {
        let x = self
            .nonce
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (&self.cipher, x.to_be_bytes())
    }

    pub fn get_cipher(&self) -> &Aes256GcmSiv {
        &self.cipher
    }
}

type EncryptionTable = HashMap<SessionId, EncryptionWindow>;

pub struct EncryptionMonitor<'a> {
    table: &'a EncryptionTable,
}

impl<'a> EncryptionMonitor<'a> {
    fn new(table: &'a EncryptionTable) -> Self {
        Self { table }
    }

    /// returns the key and nonce counter for a specific session
    ///
    /// # Panics
    /// This function panics if the key is not yet created, which should be impossible
    pub fn get(&self, session_id: &SessionId) -> (&Aes256GcmSiv, [u8; 8]) {
        self.table
            .get(session_id)
            .unwrap_or_else(|| {
                panic!(
                    "Invairant broken while accessing session table: \
        session ({session_id}) does not have a key but is being accessed for encryption",
                )
            })
            .get()
    }
    pub fn get_cipher(&self, session_id: &SessionId) -> &Aes256GcmSiv {
        self.table
            .get(session_id)
            .unwrap_or_else(|| {
                panic!(
                    "Invairant broken while accessing session table: \
        session ({session_id}) does not have a key but is being accessed for encryption",
                )
            })
            .get_cipher()
    }
}

struct SessionTable {
    encryption: EncryptionTable,
}

impl SessionTable {
    pub fn get_encryption_monitor(&self) -> EncryptionMonitor<'_> {
        EncryptionMonitor::new(&self.encryption)
    }
}

struct TmpAddressSessionMapper {
    factor: u64,
}

/// temporary monitor to give transport layer access to the address-session bindings
pub struct AddressSessionMonitor {
    table: Arc<TmpAddressSessionMapper>,
}

impl AddressSessionMonitor {
    /// deterministcally returns the session id based on a mapping
    /// from the address. Even if the session doesn't exist.
    pub fn get_session_id(&self, addr: (u8, u8, u8, u8, u16)) -> SessionId {
        let full: u64 = (addr.0 as u64) << 40
            | (addr.1 as u64) << 32
            | (addr.2 as u64) << 24
            | (addr.3 as u64) << 16
            | addr.4 as u64;
        SessionId(full * self.table.factor)
    }

    /// deterministcally returns the address based on a mapping
    /// from the session id. Even if the session doesn't exist.
    pub fn get_addr(&self, session_id: SessionId) -> String {
        #![allow(clippy::many_single_char_names)]
        let unfactored = session_id.0 / self.table.factor;
        let a = ((unfactored >> 40) & 0xFF) as u8;
        let b = ((unfactored >> 32) & 0xFF) as u8;
        let c = ((unfactored >> 24) & 0xFF) as u8;
        let d = (unfactored >> 16) & 0xFF;
        let p = (unfactored & 0xFFFF) as u16;

        format!("{a}.{b}.{c}.{d}:{p}")
    }
}

impl Default for AddressSessionMonitor {
    fn default() -> Self {
        Self {
            table: Arc::new(TmpAddressSessionMapper {
                factor: 333_333_333,
            }),
        }
    }
}
