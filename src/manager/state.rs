#![allow(private_interfaces)]
use aes_gcm_siv::Aes256GcmSiv;
use derive_more::Deref;
use tokio::sync::{Mutex, RwLock};
use x25519_dalek::EphemeralSecret;

use crate::{
    lock_read, lock_write,
    manager::packets::{BatchID, PacketFingerprint, PacketWrapper, SessionId},
    prelude::*,
};
use core::panic;
use std::{
    collections::{HashSet, VecDeque},
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

const PACKET_DISCARD_TIME_MS: u64 = 7 * 1000;

pub type GeneralStateTable = RwLock<HashMap<SessionId, GeneralSessionState>>;
pub type PendingAckTable = RwLock<HashMap<PacketFingerprint, PendingAck>>;
// encryption doesnt need a lock because key rotation semantics guarantee no read-write overlaps
pub type EncryptionTable = RwLock<HashMap<SessionId, EncryptionWindow>>;
pub type FingerprintTable = RwLock<HashMap<SessionId, Arc<FingerprintWindow>>>;
pub type FecStateTable = RwLock<HashMap<SessionId, SessionFecState>>;
pub type SessionAppIdTable = RwLock<HashMap<SessionId, AppId>>;
pub type HandshakeStateTable = RwLock<HashMap<SocketAddr, HandshakeState>>;
#[derive(Default, Deref)]
pub struct SessionPortTable(RwLock<HashMap<SessionId, SocketAddr>>);

pub struct SessionStates {
    app_id: AppId,
    port: Port,
    handles: Option<LayerHandles>,
    pub general: GeneralStateTable,
    pub handshakes: HandshakeStateTable,
    pub ack: PendingAckTable,
    pub encryption: EncryptionTable,
    pub fingerprints: FingerprintTable,
    pub addresses: SessionPortTable,
    pub fec: FecStateTable,
    pub app_ids: SessionAppIdTable,
}

impl SessionStates {
    pub fn new(
        port: Port,
        app_id: AppId,
        transport_handle: JoinHandle<()>,
        processor_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            app_id,
            port,
            handles: Some(LayerHandles::new(transport_handle, processor_handle)),
            general: GeneralStateTable::default(),
            handshakes: HandshakeStateTable::default(),
            ack: PendingAckTable::default(),
            encryption: EncryptionTable::default(),
            fingerprints: FingerprintTable::default(),
            addresses: SessionPortTable::default(),
            fec: FecStateTable::default(),
            app_ids: SessionAppIdTable::default(),
        }
    }

    pub async fn new_handshake(&self, src_addr: SocketAddr, ephemeral_secret: EphemeralSecret) {
        lock_write!(self.handshakes).insert(src_addr, HandshakeState { ephemeral_secret });
    }

    /// Joins both layer threads.
    /// **DANGEROUS**: This function blocks the entire async runtime, only use if the protocol is
    /// shutting down, when no other tasks need to be done.
    pub fn join_layers(&mut self) {
        let handles = self.handles.take().unwrap_or_else(|| {
            panic!(
                "Invariant broken while joining the layer threads: \
            function was called more than once"
            )
        });

        handles.blocking_join();
    }

    pub async fn promote_handshake(
        &self,
        new_session_id: SessionId,
        address: SocketAddr,
        app_id: AppId,
    ) -> EphemeralSecret {
        let HandshakeState { ephemeral_secret } = lock_write!(self.handshakes)
            .remove(&address)
            .unwrap_or_else(|| {
                panic!(
                    "Invariant broken while promoting handshake: \
                    handshake with {address} did not exist in state."
                )
            });

        self.new_session(SessionStateFlags::none(), new_session_id, address, app_id)
            .await;

        ephemeral_secret
    }

    pub fn app_id(&self) -> AppId {
        self.app_id.clone()
    }

    pub fn port(&self) -> Port {
        self.port
    }

    pub async fn session_exists(&self, session_id: SessionId) -> bool {
        lock_read!(self.general).contains_key(&session_id)
    }

    pub async fn new_session(
        &self,
        flags: SessionStateFlags,
        session_id: SessionId,
        address: SocketAddr,
        app_id: AppId,
    ) {
        lock_write!(self.handshakes).remove(&address);
        lock_write!(self.general).insert(
            session_id,
            GeneralSessionState {
                last_activity_time: Timestamp::now(),
                last_key_rotation_time: Timestamp::now(),
                flags,
            },
        );

        lock_write!(self.app_ids).insert(session_id, app_id);

        lock_write!(self.fingerprints).insert(session_id, Arc::new(FingerprintWindow::default()));

        lock_write!(self.addresses).insert(session_id, address);

        lock_write!(self.fec).insert(session_id, SessionFecState::default());
    }
}

struct LayerHandles {
    transport: JoinHandle<()>,
    processor: JoinHandle<()>,
}

impl LayerHandles {
    fn new(transport: JoinHandle<()>, processor: JoinHandle<()>) -> Self {
        Self {
            transport,
            processor,
        }
    }

    /// Joins both layers
    /// **DANGEROUS**: This function blocks the entire async runtime, only use if the protocol is
    /// shutting down, when no other tasks need to be done.
    fn blocking_join(self) {
        self.transport.join();
        self.processor.join();
    }
}

#[derive(Serialize, Deref, Clone, Copy)]
#[repr(transparent)]
pub struct Port(u16);

impl Port {
    pub fn new(port: u16) -> Self {
        Port(port)
    }
}

impl From<SocketAddr> for Port {
    fn from(value: SocketAddr) -> Self {
        match value {
            SocketAddr::V4(socket_addr_v4) => Port(socket_addr_v4.port()),
            SocketAddr::V6(socket_addr_v6) => Port(socket_addr_v6.port()),
        }
    }
}

impl From<SocketAddrV4> for Port {
    fn from(value: SocketAddrV4) -> Self {
        Port(value.port())
    }
}

impl From<SocketAddrV6> for Port {
    fn from(value: SocketAddrV6) -> Self {
        Port(value.port())
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
pub enum SessionStateFlag {
    Hanshake = 1 << 1,
    CurrentlyStreamingFrom = 1 << 5,
    CurrentlyStreamingTo = 1 << 6,
}

#[derive(Serialize, Debug, PartialEq, Clone, Copy)]
#[repr(transparent)]
pub struct SessionStateFlags(u32);

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

    fn none() -> Self {
        Self(0)
    }

    fn unset(mut self, flag: Self::FlagType) -> Self {
        self.0 &= !(flag as u32);
        self
    }

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

pub struct HandshakeState {
    ephemeral_secret: EphemeralSecret,
}

impl HandshakeState {
    pub fn new(ephemeral_secret: EphemeralSecret) -> Self {
        Self { ephemeral_secret }
    }
}

#[derive(Clone)]
#[repr(transparent)]
pub struct AppId(String);

impl AppId {
    pub fn new(id: String) -> Self {
        debug_assert!(
            id.is_ascii(),
            "Invariant broken while constructing `AppId`: \
            The ID is not a valid ascii sequence: {id}"
        );

        Self(id)
    }
}

impl From<&str> for AppId {
    fn from(value: &str) -> Self {
        debug_assert!(
            value.is_ascii(),
            "Invariant broken while constructing `AppId`: \
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
            buf.copy_from_slice(self.0.as_bytes());
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        let id = String::from_utf8(Vec::from(bytes)).map_err(|_| ())?;
        if id.is_ascii() {
            Ok(AppId::new(id))
        } else {
            Err(())
        }
    }

    fn sized(&self) -> usize {
        self.0.len()
    }
}

impl SessionPortTable {
    pub async fn contains(&self, id: SessionId) -> bool {
        self.read().await.contains_key(&id)
    }

    pub async fn update(&self, id: SessionId, address: SocketAddr) {
        self.write()
            .await
            .entry(id)
            .or_insert(address)
            .set_ip(address.ip());
    }
}

#[derive(Default)]
struct SessionFecState {
    table: RwLock<HashMap<BatchID, FecBatchWindow>>,
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
        self.data_arrived.set_bit(index);
    }

    #[inline]
    fn add_parity(&mut self, index: usize) {
        #[cfg(feature = "fec_xor")]
        self.recovery_arrived.set_bit(0);

        #[cfg(all(feature = "fec_rs", not(feature = "fec_xor")))]
        self.recovery_arrived.set_bit(index);
    }
}

#[derive(Default, Clone, Copy)]
struct FecArrivedBitMap([u128; 2]);

impl FecArrivedBitMap {
    /// Sets the bit of the given index
    #[inline]
    fn set_bit(&mut self, index: usize) {
        self.0[index / 128] |= 1 << (index % 128);
    }

    /// Returns true if enough bits are set based on a specified threshold
    #[inline]
    fn enough_set(&self, threshold: u8) -> bool {
        self.0[0].count_ones() + self.0[1].count_ones() >= threshold as u32
    }

    /// Returns true if the bit under the specified index is set
    #[inline]
    fn is_set(&self, index: usize) -> bool {
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

#[derive(Clone, Copy)]
pub struct PendingAckMonitor {
    table: &'static PendingAckTable,
}

impl PendingAckMonitor {
    pub fn new(table: &'static PendingAckTable) -> Self {
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

#[derive(Clone, Copy)]
pub struct FingerprintMonitor {
    table: &'static FingerprintTable,
}

impl FingerprintMonitor {
    pub fn new(table: &'static FingerprintTable) -> Self {
        Self { table }
    }

    pub async fn add(&self, session_id: SessionId) {
        let mut table = self.table.write().await;
        table.insert(session_id, Arc::default());
    }

    /// returns an Arc to the window for this session
    ///
    /// # Panics
    /// This function panics if the session is not yet initialized - an Invariant
    pub async fn get(&self, session_id: &SessionId) -> Arc<FingerprintWindow> {
        let table = self.table.read().await;
        let Some(window) = table.get(session_id) else {
            panic!(
                "Invariant broken while trying to get a `FingerprintWindow`:\
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
    cipher: Arc<Aes256GcmSiv>,
    nonce: AtomicU64,
}

impl EncryptionWindow {
    pub fn new(cipher: Aes256GcmSiv) -> Self {
        Self {
            cipher: Arc::new(cipher),
            nonce: AtomicU64::new(0),
        }
    }

    pub fn get(&self) -> (Arc<Aes256GcmSiv>, [u8; 8]) {
        let x = self
            .nonce
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        (self.cipher.clone(), x.to_be_bytes())
    }

    pub fn get_cipher(&self) -> Arc<Aes256GcmSiv> {
        self.cipher.clone()
    }
}

#[derive(Clone, Copy)]
pub struct EncryptionMonitor {
    table: &'static EncryptionTable,
}

impl EncryptionMonitor {
    pub fn new(table: &'static EncryptionTable) -> Self {
        Self { table }
    }

    /// returns the key and nonce counter, increasing it in the process, for a specific session
    ///
    /// # Panics
    /// This function panics if the key is not yet created, which should be impossible
    pub async fn get(&self, session_id: &SessionId) -> (Arc<Aes256GcmSiv>, [u8; 8]) {
        self.table
            .write()
            .await
            .get(session_id)
            .unwrap_or_else(|| {
                panic!(
                    "Invariant broken while accessing session table: \
        session ({session_id}) does not have a key but is being accessed for encryption",
                )
            })
            .get()
    }

    /// returns the key without increasing the counter
    ///
    /// # Panics
    /// This function panics if the key is not yet created, which should be impossible
    pub async fn get_cipher(&self, session_id: &SessionId) -> Arc<Aes256GcmSiv> {
        self.table
            .read()
            .await
            .get(session_id)
            .unwrap_or_else(|| {
                panic!(
                    "Invariant broken while accessing session table: \
        session ({session_id}) does not have a key but is being accessed for encryption",
                )
            })
            .get_cipher()
    }
}
