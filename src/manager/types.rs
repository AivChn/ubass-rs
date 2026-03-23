use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use tokio::sync::{Mutex, RwLock};
use uniffi::{ForeignBytes, foreignbytes};

use crate::packetizer::{
    fingerprint,
    types::{PacketFingerprint, SessionId, Timestamp},
};

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

#[derive(Eq)]
struct FingerprintPtr(*const PacketFingerprint);

unsafe impl Send for FingerprintPtr {}
unsafe impl Sync for FingerprintPtr {}

impl FingerprintPtr {
    fn from_box(value: &PacketFingerprint) -> Self {
        Self(std::ptr::from_ref(value))
    }
}

impl PartialEq for FingerprintPtr {
    fn eq(&self, other: &Self) -> bool {
        unsafe { (*self.0) == (*other.0) }
    }
}

impl Hash for FingerprintPtr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            (*self.0).hash(state);
        }
    }
}

pub struct FingerprintMonitor {
    table: RwLock<HashMap<SessionId, Arc<FingerprintWindow>>>,
}

impl Default for FingerprintMonitor {
    fn default() -> Self {
        Self {
            table: RwLock::new(HashMap::new()),
        }
    }
}

impl FingerprintMonitor {
    pub async fn add(&self, session_id: SessionId) {
        let mut table = self.table.write().await;
        table.insert(session_id, Arc::default());
    }

    /// returns an Arc to the window for this session
    ///
    /// # Panics
    /// This function panics if the session is not yet initialized - an invairant
    pub async fn get(&self, session_id: SessionId) -> Arc<FingerprintWindow> {
        let table = self.table.read().await;
        let Some(window) = table.get(&session_id) else {
            panic!(
                "Invairant broken while trying to get a `FingerprintWindow`:\
            {session_id} is not a valid session"
            );
        };

        window.clone()
    }
}

pub struct FingerprintWindow {
    fingerprints: Mutex<HashSet<Box<PacketFingerprint>>>,
    queue: Mutex<VecDeque<(Timestamp, FingerprintPtr)>>,
    canceled: AtomicBool,
}

impl Default for FingerprintWindow {
    fn default() -> Self {
        Self {
            fingerprints: Mutex::new(HashSet::new()),
            queue: Mutex::new(VecDeque::new()),
            canceled: AtomicBool::new(false),
        }
    }
}

impl FingerprintWindow {
    const PRUNE_INTERVAL: u64 = 7 * 1000;

    pub fn init(self: Arc<Self>) {
        tokio::spawn(self.prune());
    }

    pub async fn add(&self, fingerprint: Box<PacketFingerprint>) -> bool {
        let ptr = 'add_to_set: {
            let mut fingerprints = self.fingerprints.lock().await;
            let ptr = FingerprintPtr::from_box(&fingerprint);
            if fingerprints.insert(fingerprint) {
                Some(ptr)
            } else {
                None
            }
        };

        let Some(ptr) = ptr else {
            return false;
        };

        'add_to_queue: {
            let mut queue = self.queue.lock().await;
            queue.push_back((Timestamp::now(), ptr));
        }

        true
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired = Vec::with_capacity(256);
        while !self.canceled.load(Ordering::Relaxed) {
            'pop_queue: {
                let mut queue = self.queue.lock().await;
                while queue
                    .front()
                    .is_some_and(|(ts, _)| ts.been_longer_than(Self::PRUNE_INTERVAL))
                {
                    if let Some((_, value)) = queue.pop_front() {
                        expired.push(value);
                    }
                }
            }

            'remove: {
                let mut fingerprints = self.fingerprints.lock().await;
                expired
                    .drain(..)
                    .for_each(|value| _ = fingerprints.remove(unsafe { &*value.0 }));
            }

            tokio::time::sleep(Duration::from_millis(Self::PRUNE_INTERVAL));
        }
    }
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

    /// returns the key and nonce counter, increasing it in the process, for a specific session
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

    /// returns the key without increasing the counter
    ///
    /// # Panics
    /// This function panics if the key is not yet created, which should be impossible
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
