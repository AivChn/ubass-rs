use aes_gcm_siv::Aes256GcmSiv;
use derive_more::Deref;
use tokio::sync::{Mutex, RwLock};

use crate::{
    manager::packets::{BatchID, PacketFingerprint, PacketWrapper, SessionId},
    prelude::*,
};
use std::{
    collections::{HashSet, VecDeque},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

const PACKET_DISCARD_TIME_MS: u64 = 7 * 1000;

type GeneralStateTable = RwLock<HashMap<SessionId, GeneralSessionState>>;
type PendingAckTable = RwLock<HashMap<PacketFingerprint, PendingAck>>;
// encryption doesnt need a lock because key rotation semantics guarantee no read-write overlaps
type EncryptionTable = HashMap<SessionId, EncryptionWindow>;
type FingerprintTable = RwLock<HashMap<SessionId, Arc<FingerprintWindow>>>;
type FecStateTable = RwLock<HashMap<SessionId, SessionFecState>>;
type SessionAppIdTable = RwLock<HashMap<SessionId, AppId>>;
#[derive(Deref)]
struct SessionAddressTable(RwLock<HashMap<SessionId, SocketAddr>>);

struct SessionStates {
    general: GeneralStateTable,
    ack: PendingAckTable,
    encryption: EncryptionTable,
    fingerprints: FingerprintTable,
    addresses: SessionAddressTable,
    fec: FecStateTable,
    app_ids: SessionAppIdTable,
}

impl Default for SessionStates {
    fn default() -> Self {
        Self {
            general: GeneralStateTable::default(),
            ack: PendingAckTable::default(),
            encryption: EncryptionTable::default(),
            fingerprints: FingerprintTable::default(),
            addresses: SessionAddressTable::default(),
            fec: FecStateTable::default(),
            app_ids: SessionAppIdTable::default(),
        }
    }
}

struct GeneralSessionState {
    last_activity_time: Timestamp,
    last_key_rotation_time: Timestamp,
    flags: SessionStateFlags,
}

#[derive(PartialEq, Clone, Copy)]
#[repr(u32)]
#[variants_array]
enum SessionStateFlag {
    Handshake = 1 << 0,
    CurrentlyStreamingFrom = 1 << 5,
    CurrentlyStreamingTo = 1 << 6,
}

#[derive(Serialize, Debug, PartialEq, Clone, Copy)]
#[repr(transparent)]
struct SessionStateFlags(u32);

impl Flags for SessionStateFlags {
    type FlagType = SessionStateFlag;

    #[inline]
    fn construct(flags: &[Self::FlagType]) -> Self {
        Self(
            flags
                .iter()
                .map(|x| *x as u32)
                .reduce(|f1, f2| f1 | f2)
                .unwrap_or(0),
        )
    }

    #[must_use]
    fn unset(mut self, flag: Self::FlagType) -> Self {
        self.0 &= !(flag as u32);
        self
    }

    #[must_use]
    fn set(mut self, flag: Self::FlagType) -> Self {
        self.0 |= flag as u32;
        self
    }

    #[inline]
    fn contains(self, flag: Self::FlagType) -> bool {
        self.0 & (flag as u32) != 0
    }

    #[inline]
    fn deconstruct(self) -> Vec<Self::FlagType> {
        Self::FlagType::VARIANTS
            .iter()
            .copied()
            .filter(|e| (*e as u32) & self.0 != 0)
            .collect()
    }
}

#[repr(transparent)]
pub struct AppId(String);

impl AppId {
    fn new(id: String) -> Self {
        debug_assert!(
            id.is_ascii(),
            "Invairant broken while constructing `AppId`: \
            The ID is not a valid ascii sequence: {id}"
        );

        Self(id)
    }

    fn from_slice(id: &str) -> Self {
        id.into()
    }
}

impl From<&str> for AppId {
    fn from(value: &str) -> Self {
        debug_assert!(
            value.is_ascii(),
            "Invairant broken while constructing `AppId` from &str: \
            The ID is not a valid ascii sequence: {value}"
        );

        Self(String::from(value))
    }
}

impl Serialize for AppId {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.0.len() {
            Err(())
        } else {
            buf.copy_from_slice(&self.0.as_bytes());
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        Ok(AppId(String::from_utf8(Vec::from(bytes)).map_err(|_| ())?))
    }

    fn sized(&self) -> usize {
        self.0.len()
    }
}

impl Default for SessionAddressTable {
    fn default() -> Self {
        Self(RwLock::default())
    }
}

impl SessionAddressTable {
    pub async fn contains(&self, id: SessionId) -> bool {
        self.read().await.contains_key(&id)
    }

    pub async fn update(&self, id: SessionId, address: SocketAddr) {
        self.write().await.insert(id, address);
    }
}

struct SessionFecState {
    table: RwLock<HashMap<BatchID, FecBatchWindow>>,
}

impl Default for SessionFecState {
    fn default() -> Self {
        Self {
            table: RwLock::default(),
        }
    }
}

struct FecBatchWindow {
    batch_size: u8,
    recovery_count: u8,
    data_arrived: FecArrivedBitMap,
    recovery_arrived: FecArrivedBitMap,
}

impl FecBatchWindow {
    fn new(batch_size: u8, recovery_count: u8) -> Self {
        Self {
            batch_size,
            recovery_count,
            data_arrived: FecArrivedBitMap::default(),
            recovery_arrived: FecArrivedBitMap::default(),
        }
    }

    fn revovery_ready(&self) -> bool {
        let needed_data = self.batch_size - self.recovery_count;
        self.data_arrived.enough_set(needed_data)
    }

    #[inline]
    fn add_data(&mut self, index: usize) {
        self.data_arrived.set(index);
    }

    #[inline]
    fn add_parity(&mut self, index: usize) {
        #[cfg(feature = "fec_xor")]
        self.recovery_arrived.set(0);

        #[cfg(feature = "fec_rs")]
        self.recovery_arrived.set(index);
    }
}

#[derive(Clone, Copy)]
struct FecArrivedBitMap([u128; 2]);

impl Default for FecArrivedBitMap {
    fn default() -> Self {
        Self([0; 2])
    }
}

impl FecArrivedBitMap {
    #[inline]
    fn set(&mut self, index: usize) {
        self.0[index / 128] |= 1 << (index % 128);
    }

    #[inline]
    fn enough_set(&self, threshold: u8) -> bool {
        self.0[0].count_ones() + self.0[1].count_ones() >= threshold as u32
    }

    #[inline]
    fn contains(&self, index: usize) -> bool {
        (self.0[index / 128] >> (index % 128)) % 2 == 1
    }
}

pub struct PendingAck {
    packet: PacketWrapper,
    timestamp: Timestamp,
    retries: u8,
}

impl PendingAck {
    const MAX_RETRIES: u8 = 5;

    pub fn new(packet: PacketWrapper, timestamp: Timestamp) -> Self {
        Self {
            packet,
            timestamp,
            retries: 0,
        }
    }

    pub fn packet(&self) -> &PacketWrapper {
        &self.packet
    }

    pub fn retried(&mut self) {
        self.timestamp = Timestamp::now();
        self.retries += 1;
    }

    pub fn is_expired(&self) -> bool {
        self.timestamp.been_longer_than(PACKET_DISCARD_TIME_MS) && self.retries >= Self::MAX_RETRIES
    }
}

pub struct PendingAckMonitor<'a> {
    table: &'a PendingAckTable,
}

impl<'a> PendingAckMonitor<'a> {
    pub fn new(table: &'a PendingAckTable) -> Self {
        Self { table }
    }

    pub async fn add(
        &self,
        fingerprint: PacketFingerprint,
        pending_ack: (PacketWrapper, Timestamp),
    ) {
        let mut table = self.table.write().await;
        table.insert(fingerprint, PendingAck::new(pending_ack.0, pending_ack.1));
    }
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

impl core::hash::Hash for FingerprintPtr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            (*self.0).hash(state);
        }
    }
}

pub struct FingerprintMonitor<'a> {
    table: &'a FingerprintTable,
}

impl<'a> FingerprintMonitor<'a> {
    pub fn new(table: &'a FingerprintTable) -> Self {
        Self { table }
    }

    pub async fn add(&self, session_id: SessionId) {
        let mut table = self.table.write().await;
        table.insert(session_id, Arc::default());
    }

    /// returns an Arc to the window for this session
    ///
    /// # Panics
    /// This function panics if the session is not yet initialized - an invairant
    pub async fn get(&self, session_id: &SessionId) -> Arc<FingerprintWindow> {
        let table = self.table.read().await;
        let Some(window) = table.get(session_id) else {
            panic!(
                "Invairant broken while trying to get a `FingerprintWindow`:\
            {session_id} is not a valid session"
            );
        };

        window.clone()
    }
}

pub struct FingerprintWindow {
    fingerprints: RwLock<HashSet<Box<PacketFingerprint>>>,
    queue: Mutex<VecDeque<(Timestamp, FingerprintPtr)>>,
    canceled: AtomicBool,
}

impl Default for FingerprintWindow {
    fn default() -> Self {
        Self {
            fingerprints: RwLock::new(HashSet::new()),
            queue: Mutex::new(VecDeque::new()),
            canceled: AtomicBool::new(false),
        }
    }
}

impl FingerprintWindow {
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;
    const BUFFERING_TIME: u64 = 2 * 1000;

    pub fn init(self: Arc<Self>) {
        tokio::spawn(self.prune());
    }

    #[must_use]
    pub async fn contains(&self, fingerprint: &PacketFingerprint) -> bool {
        let fingerprints = self.fingerprints.read().await;
        fingerprints.contains(fingerprint)
    }

    pub async fn add(&self, fingerprint: Box<PacketFingerprint>) -> bool {
        let ptr = {
            let mut fingerprints = self.fingerprints.write().await;
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

        {
            let mut queue = self.queue.lock().await;
            queue.push_back((Timestamp::now(), ptr));
        }

        true
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired = Vec::with_capacity(256);
        while !self.canceled.load(Ordering::Relaxed) {
            let top_timestamp;
            {
                let mut queue = self.queue.lock().await;
                while queue
                    .front()
                    .is_some_and(|(ts, _)| ts.been_longer_than(Self::PRUNE_INTERVAL))
                {
                    if let Some((_, value)) = queue.pop_front() {
                        expired.push(value);
                    }
                }
                top_timestamp = {
                    if let Some(top) = queue.front() {
                        top.0.get()
                    } else {
                        Self::PRUNE_INTERVAL - Self::BUFFERING_TIME
                    }
                }
            }

            {
                let mut fingerprints = self.fingerprints.write().await;
                expired
                    .drain(..)
                    .for_each(|value| _ = fingerprints.remove(unsafe { &*value.0 }));
            }

            tokio::time::sleep(Duration::from_millis(top_timestamp + Self::BUFFERING_TIME)).await;
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
