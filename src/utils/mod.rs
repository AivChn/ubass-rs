use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinHandle};

pub mod messages;

pub use messages::*;

#[macro_export]
macro_rules! o_unwrap_or_return {
    ($result:expr) => {
        match $result {
            Some(val) => val,
            None => return,
        }
    };
}

#[macro_export]
macro_rules! r_unwrap_or_return {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(_) => return,
        }
    };
}

#[macro_export]
macro_rules! dispatch {
    ($call:expr => $monitor:expr) => {
        let handle = tokio::spawn($call);
        $monitor.add(handle).await;
    };
}

#[macro_export]
macro_rules! lock_read {
    ($to_lock:expr) => {
        $to_lock.read().await
    };
}

#[macro_export]
macro_rules! lock_write {
    ($to_lock:expr) => {
        $to_lock.write().await
    };
}

pub trait Flags {
    type FlagType;
    fn construct(flags: &[Self::FlagType]) -> Self;
    fn deconstruct(self) -> Vec<Self::FlagType>;
    fn none() -> Self;
    #[must_use]
    fn set(self, flag: Self::FlagType) -> Self;
    #[must_use]
    fn unset(self, flag: Self::FlagType) -> Self;
    fn contains(self, flag: Self::FlagType) -> bool;
}

#[async_trait]
pub trait SendPacket {
    type Sender;

    async fn send(self: Box<Self>, sender: Self::Sender, address: SocketAddr);
}

pub struct W<T>(pub T);

pub struct HandleMonitor {
    handles: Mutex<Vec<JoinHandle<()>>>,
    destroyed: AtomicBool,
}

impl HandleMonitor {
    const PRUNE_INTERVAL: u64 = 1000;

    pub async fn size(&self) -> usize {
        let mut handles = self.handles.lock().await;
        handles.retain(|h| !h.is_finished());
        handles.len()
    }

    pub async fn init(self: Arc<Self>) {
        while !self.destroyed.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(Self::PRUNE_INTERVAL)).await;
            self.prune().await;
        }
    }

    pub async fn add(&self, handle: JoinHandle<()>) {
        let mut handles = self.handles.lock().await;
        handles.push(handle);
    }

    pub async fn prune(&self) {
        let mut handles = self.handles.lock().await;
        handles.retain(|h| !h.is_finished());
    }

    pub async fn flush(&self) {
        self.destroyed
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            _ = handle.await;
        }
    }
}

impl Default for HandleMonitor {
    fn default() -> Self {
        Self {
            handles: Mutex::new(Vec::with_capacity(32)),
            destroyed: AtomicBool::new(false),
        }
    }
}
