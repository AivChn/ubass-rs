#![allow(private_interfaces)]
use aes_gcm_siv::Aes256GcmSiv;
use derive_more::Deref;
use tokio::sync::{Mutex, RwLock};
use x25519_dalek::EphemeralSecret;

use crate::{
    debug_o_unwrap_or_return, debug_r_unwrap_or_return, get_state, lock, lock_read, lock_write,
    manager::{
        STATE, inbound,
        packets::{
            BatchID, HelloPacket, MAX_PAYLOAD_LENGTH, Packet, PacketFingerprint, PacketWrapper,
            SessionId,
        },
        types::OutboundSender,
    },
    o_unwrap_or_return,
    packet_processor::fingerprint,
    prelude::*,
    r_unwrap_or_return,
};
use core::panic;
use std::{
    collections::{HashSet, VecDeque, hash_map::Entry},
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

const PACKET_DISCARD_TIME_MS: u64 = 7 * 1000;

macro_rules! sessions_state_fields {
    ($($name:ident($key:ty => $value:ty)),*) => {
        $(
            #[derive(Default, Deref)]
            pub struct $name(RwLock<HashMap<$key, $value>>);
        )*
    };
}

sessions_state_fields!(
    GeneralStateTable(SessionId => GeneralSessionState),
    EncryptionTable(SessionId => EncryptionWindow),
    FingerprintTable(SessionId => Arc<FingerprintWindow>),
    FecStateTable(SessionId => SessionFecState),
    SessionAppIdTable(SessionId => AppId),
    SessionAddressTable(SessionId => SocketAddr),
    HandshakeStateTable(HandshakeId => HandshakeState)
);

#[derive(Default)]
pub struct LastActivityTable(HashMap<SessionId, Timestamp>);

pub struct SessionStates {
    app_id: AppId,
    port: Port,
    handles: Option<LayerHandles>,
    global_handle_monitor: Arc<HandleMonitor>,
    pub general: GeneralStateTable,
    pub last_activity: LastActivityTable,
    pub handshakes: HandshakeStateTable,
    pub ack: PendingAckWindow,
    pub encryption: EncryptionTable,
    pub fingerprints: FingerprintTable,
    pub addresses: SessionAddressTable,
    pub fec: FecStateTable,
    pub app_ids: SessionAppIdTable,
}

impl SessionStates {
    pub fn new(
        port: Port,
        app_id: AppId,
        sender: OutboundSender,
        transport_handle: JoinHandle<()>,
        processor_handle: JoinHandle<()>,
    ) -> Self {
        let global_handle_monitor = Arc::new(HandleMonitor::default());
        global_handle_monitor.clone().init();

        Self {
            app_id,
            port,
            handles: Some(LayerHandles::new(transport_handle, processor_handle)),
            global_handle_monitor,
            general: GeneralStateTable::default(),
            last_activity: LastActivityTable::default(),
            handshakes: HandshakeStateTable::default(),
            ack: PendingAckWindow::new(sender),
            encryption: EncryptionTable::default(),
            fingerprints: FingerprintTable::default(),
            addresses: SessionAddressTable::default(),
            fec: FecStateTable::default(),
            app_ids: SessionAppIdTable::default(),
        }
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
        handshake_id: HandshakeId,
        app_id: AppId,
    ) -> EphemeralSecret {
        let Some(HandshakeState {
            peer_address,
            ephemeral_secret,
            session_id,
        }) = self.handshakes.take(handshake_id).await
        else {
            unreachable!()
        };

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
        lock_write!(self.general).insert(
            session_id,
            GeneralSessionState {
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

pub struct GeneralSessionState {
    last_key_rotation_time: Timestamp,
    flags: SessionStateFlags,
}

impl GeneralStateTable {
    pub async fn last_key_rotation_time(&self, session_id: SessionId) -> Option<Timestamp> {
        Some(lock_read!(self).get(&session_id)?.last_key_rotation_time)
    }

    pub async fn key_rotation(&self, session_id: SessionId) -> Option<()> {
        lock_write!(self)
            .get_mut(&session_id)?
            .last_key_rotation_time = Timestamp::now();
        Some(())
    }

    pub async fn flags(&self, session_id: SessionId) -> Option<SessionStateFlags> {
        Some(lock_read!(self).get(&session_id)?.flags)
    }

    pub async fn flags_then(
        &self,
        session_id: SessionId,
        flag: <SessionStateFlags as Flags>::FlagType,
        mut f: impl FnMut(
            SessionStateFlags,
            <SessionStateFlags as Flags>::FlagType,
        ) -> SessionStateFlags,
    ) -> Option<()> {
        let mut lock = lock_write!(self);
        let flags = lock.get(&session_id)?.flags;
        let new = f(flags, flag);
        lock.get_mut(&session_id)?.flags = new;
        Some(())
    }
}

impl LastActivityTable {
    pub fn update(&self, session_id: SessionId) {
        unsafe {
            let this = self as *const Self as *mut Self;
            (*this)
                .0
                .entry(session_id)
                .and_modify(Timestamp::set_again)
                .or_insert_with(Timestamp::now);
        }
    }

    pub fn read(&self, session_id: SessionId) -> Timestamp {
        self.0
            .get(&session_id)
            .copied()
            .unwrap_or_else(Timestamp::now)
    }
}

#[derive(Flags, Clone, Copy)]
#[repr(transparent)]
#[flagtype(AppOptionFlag)]
pub struct AppOptions(u32);

#[derive(Clone, Copy)]
#[repr(u32)]
#[variants_array]
pub enum AppOptionFlag {
    ApproveAllApps = 1 << 0,
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

#[derive(Debug, Serialize, Deref, Clone, Copy)]
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

#[derive(PartialEq, Clone, Copy)]
#[repr(u32)]
#[variants_array]
pub enum SessionStateFlag {
    Handshake = 1 << 1,
    CurrentlyStreamingFrom = 1 << 5,
    CurrentlyStreamingTo = 1 << 6,
}

#[derive(Flags, Serialize, Debug, PartialEq, Clone, Copy)]
#[repr(transparent)]
#[flagtype(SessionStateFlag)]
pub struct SessionStateFlags(u32);

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy, Serialize)]
#[repr(transparent)]
pub struct HandshakeId(u32);

impl HandshakeId {
    pub async fn generate() -> Self {
        let lock = lock_read!(get_state!().handshakes);
        loop {
            let r = Self(rand::random::<u32>());
            if !lock.contains_key(&r) {
                return r;
            }
        }
    }
}

pub struct HandshakeState {
    peer_address: SocketAddr,
    ephemeral_secret: EphemeralSecret,
    session_id: SessionId,
}

impl HandshakeState {
    pub fn new(
        peer_address: SocketAddr,
        ephemeral_secret: EphemeralSecret,
        session_id: SessionId,
    ) -> Self {
        Self {
            peer_address,
            ephemeral_secret,
            session_id,
        }
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct AppId(String);

impl AppId {
    // Large enough without exceeding the mac packet size with the number of headers on the
    // HelloPacket
    pub const MAX_LENGTH: usize = 512;
    pub fn new(id: String) -> Self {
        debug_assert!(
            id.is_ascii(),
            "Invariant broken while constructing `AppId`: \
            The ID is not a valid ascii sequence: {id}"
        );

        debug_assert!(
            id.len() < Self::MAX_LENGTH,
            "Invariant broken while constructing `AppId`: \
                The ID is larger than `Self::MAX_LENGTH` ({} >= {})",
            id.len(),
            Self::MAX_LENGTH
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

        debug_assert!(
            value.len() < Self::MAX_LENGTH,
            "Invariant broken while constructing `AppId`: \
                The ID is larger than `Self::MAX_LENGTH` ({} >= {})",
            value.len(),
            Self::MAX_LENGTH
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

impl HandshakeStateTable {
    pub async fn new_handshake(
        &self,
        handshake_id: HandshakeId,
        peer_address: SocketAddr,
        ephemeral_secret: EphemeralSecret,
        session_id: SessionId,
    ) {
        lock_write!(self).insert(
            handshake_id,
            HandshakeState::new(peer_address, ephemeral_secret, session_id),
        );
    }

    pub async fn take(&self, id: HandshakeId) -> Option<HandshakeState> {
        lock_write!(self).remove(&id)
    }
}

impl SessionAddressTable {
    pub async fn contains(&self, id: SessionId) -> bool {
        self.read().await.contains_key(&id)
    }

    pub async fn address_changed(&self, id: SessionId, address: SocketAddr) -> bool {
        !matches!(lock_read!(self).get(&id), Some(addr) if *addr == address)
    }

    pub async fn update(&self, id: SessionId, address: SocketAddr) -> SocketAddr {
        let mut lock = lock_write!(self);
        let addr = lock.entry(id).or_insert(address);
        addr.set_ip(address.ip());
        *addr
    }
}

#[derive(Default)]
pub struct SessionFecState {
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
        self.data_arrived.count_set() + self.recovery_arrived.count_set() >= self.batch_size
    }

    #[inline]
    fn add_data(&mut self, index: usize) {
        self.data_arrived.set_bit(index);
    }

    #[inline]
    #[cfg(feature = "fec_xor")]
    fn add_parity(&mut self) {
        self.recovery_arrived.set_bit(0);
    }

    #[inline]
    #[cfg(all(feature = "fec_rs", not(feature = "fec_xor")))]
    fn add_parity(&mut self, index: usize) {
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

    fn count_set(&self) -> u8 {
        (self.0[0].count_ones() + self.0[1].count_ones()) as u8
    }

    /// Returns true if the bit under the specified index is set
    #[inline]
    fn is_set(&self, index: usize) -> bool {
        (self.0[index / 128] >> (index % 128)) % 2 == 1
    }
}

#[derive(Clone, Copy)]
pub struct PendingAckMonitor {
    table: &'static PendingAckWindow,
}

impl PendingAckMonitor {
    pub fn new(table: &'static PendingAckWindow) -> Self {
        Self { table }
    }

    pub async fn add(&self, packet: Packet) {
        self.table.add(packet).await;
    }
}

#[derive(Eq, Deref)]
struct FingerprintPtr(*const PacketFingerprint);

unsafe impl Send for FingerprintPtr {}
unsafe impl Sync for FingerprintPtr {}

impl FingerprintPtr {
    fn from_ref(value: &PacketFingerprint) -> Self {
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

//impl PendingAck {
//    const MAX_RETRIES: u8 = 5;
//
//    pub fn new(packet: PacketWrapper, timestamp: Timestamp) -> Self {
//        Self {
//            packet,
//            timestamp,
//            retries: 0,
//        }
//    }
//
//    pub fn packet(&self) -> PacketWrapper {
//        self.packet.clone()
//    }
//
//    pub fn retried(&mut self) {
//        self.timestamp = Timestamp::now();
//        self.retries += 1;
//    }
//
//    pub fn is_expired(&self) -> bool {
//        self.timestamp.been_longer_than(PACKET_DISCARD_TIME_MS) && self.retries >= Self::MAX_RETRIES
//    }
//}
//
//impl PendingAckTable {}

struct PendingAckQueueEntry {
    timestamp: Timestamp,
    ptr: FingerprintPtr,
    retries: u8,
}

impl PendingAckQueueEntry {
    const MAX_RETRIES: u8 = 5;
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;

    fn new(ptr: &PacketFingerprint) -> Self {
        Self {
            timestamp: Timestamp::now(),
            ptr: FingerprintPtr::from_ref(ptr),
            retries: 0,
        }
    }

    fn retried(&mut self) -> bool {
        self.retries += 1;
        self.retries > Self::MAX_RETRIES
    }

    #[inline]
    fn ready_to_retry(&self) -> bool {
        self.timestamp.been_longer_than(Self::PRUNE_INTERVAL)
    }
}

pub struct PendingAckWindow {
    pending: RwLock<HashMap<PacketFingerprint, Packet>>,
    queue: Mutex<VecDeque<PendingAckQueueEntry>>,
    sender: OutboundSender,
    canceled: AtomicBool,
}

impl PendingAckWindow {
    const PRUNE_INTERVAL: u64 = PACKET_DISCARD_TIME_MS;
    const BUFFERING_TIME: u64 = 2 * 1000;

    pub fn new(sender: OutboundSender) -> Self {
        Self {
            pending: RwLock::default(),
            queue: Mutex::default(),
            sender,
            canceled: AtomicBool::new(false),
        }
    }

    pub async fn init(self: Arc<Self>) {
        get_state!().global_handle_monitor.dispatch(self.prune());
    }

    pub async fn add(&'static self, packet: Packet) {
        get_state!()
            .global_handle_monitor
            .dispatch(self._add(packet))
            .await;
    }

    async fn _add(&self, packet: Packet) {
        let fingerprint = debug_r_unwrap_or_return!(
            PacketFingerprint::try_from(&packet),
            format!(
                "Invariant broken while adding a packet to `PendingAckWindow`:\
                A packet that should not be acked was provided ({:?}) full list can\
                be found at the impl TryFrom<&Packet> for PacketFingerprint",
                packet
            )
        );

        let entry = PendingAckQueueEntry::new(&fingerprint);
        lock_write!(self.pending).insert(fingerprint, packet);
        lock!(self.queue).push_back(entry);
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired = Vec::with_capacity(256);
        let mut to_retry = Vec::with_capacity(256);

        while !self.canceled.load(Ordering::Relaxed) {
            let top_timestamp = {
                // get expired pending ack packets as well as ones to retry
                let mut queue = lock!(self.queue);
                while queue
                    .front()
                    .is_some_and(PendingAckQueueEntry::ready_to_retry)
                {
                    if let Some(mut value) = queue.pop_front() {
                        if value.retried() {
                            expired.push(value.ptr);
                        } else {
                            value.timestamp.set_again();
                            to_retry.push(value);
                        }
                    }
                }

                // return the time until next pending ack needs a retry
                match queue.front() {
                    Some(top) => Timestamp::now().get() - top.timestamp.get(),
                    None => Self::PRUNE_INTERVAL - Self::BUFFERING_TIME,
                }
            };

            // resend pending acks
            {
                let pending = lock_read!(self.pending);
                let mut queue = lock!(self.queue);
                let lock = lock_read!(get_state!().addresses);
                for entry in to_retry.drain(..) {
                    let Some(packet) = pending.get(unsafe { &**entry.ptr }) else {
                        continue;
                    };

                    let Some(address) = lock.get(
                        &packet
                            .session_id()
                            .expect("IncompatibleVersion packets are never acked"),
                    ) else {
                        continue;
                    };

                    queue.push_back(entry);

                    Box::new(packet.clone()).send(self.sender.clone(), *address);
                }
            }

            // remove expired acks
            {
                let mut pending = lock_write!(self.pending);
                expired
                    .drain(..)
                    .for_each(|ptr| _ = pending.remove(unsafe { &**ptr }));
            }

            tokio::time::sleep(Duration::from_millis(top_timestamp + Self::BUFFERING_TIME)).await;
        }
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

    pub async fn init(self: Arc<Self>) {
        get_state!()
            .global_handle_monitor
            .dispatch(self.prune())
            .await;
    }

    #[must_use]
    pub async fn contains(&self, fingerprint: &PacketFingerprint) -> bool {
        let fingerprints = self.fingerprints.read().await;
        fingerprints.contains(fingerprint)
    }

    pub async fn add(&self, fingerprint: Box<PacketFingerprint>) -> bool {
        let ptr = {
            let mut fingerprints = lock_write!(self.fingerprints);
            let ptr = FingerprintPtr::from_ref(&fingerprint);
            if !fingerprints.insert(fingerprint) {
                return false;
            }

            ptr
        };

        let mut queue = self.queue.lock().await;
        queue.push_back((Timestamp::now(), ptr));

        true
    }

    pub async fn prune(self: Arc<Self>) {
        let mut expired = Vec::with_capacity(256);
        while !self.canceled.load(Ordering::Relaxed) {
            let top_timestamp = {
                let mut queue = self.queue.lock().await;
                while queue
                    .front()
                    .is_some_and(|(ts, _)| ts.been_longer_than(Self::PRUNE_INTERVAL))
                {
                    if let Some((_, ptr)) = queue.pop_front() {
                        expired.push(ptr);
                    }
                }
                match queue.front() {
                    Some(top) => Timestamp::now().get() - top.0.get(),
                    None => Self::PRUNE_INTERVAL - Self::BUFFERING_TIME,
                }
            };

            {
                let mut fingerprints = self.fingerprints.write().await;
                expired
                    .drain(..)
                    .for_each(|ptr| _ = fingerprints.remove(unsafe { &**ptr }));
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
