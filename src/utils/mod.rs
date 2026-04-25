use std::{
    any::Any,
    net::SocketAddr,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use tokio::{sync::Mutex, task::JoinHandle};

pub mod messages;

pub use messages::*;

#[macro_export]
macro_rules! match_or_return {
    ($value:expr, $i:ident, $p:pat) => {
        match $value {
            $p => $i,
            _ => return,
        }
    };
}

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
macro_rules! debug_r_unwrap_or_return {
    ($result:expr, $msg:expr) => {
        match $result {
            Ok(val) => val,
            Err(_) => {
                #[cfg(debug_assertions)]
                panic!("Invariant broken: {}.", $msg);
                return;
            }
        }
    };
}

#[macro_export]
macro_rules! debug_o_unwrap_or_return {
    ($result:expr, $msg:expr) => {
        match $result {
            Some(val) => val,
            None => {
                debug_assert!(false, "Invariant broken: {}.", $msg);
                return;
            }
        }
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

#[macro_export]
macro_rules! lock {
    ($to_lock:expr) => {
        $to_lock.lock().await
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
    #[cfg(debug_assertions)]
    fn valid_flag(self) -> bool;
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

    pub fn init(self: Arc<Self>) {
        tokio::spawn(self.prune());
    }

    #[inline]
    pub async fn dispatch<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.handles.lock().await.push(tokio::spawn(future));
    }

    pub async fn prune(self: Arc<Self>) {
        while !self.destroyed.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(Self::PRUNE_INTERVAL)).await;
            let mut handles = self.handles.lock().await;
            handles.retain(|h| !h.is_finished());
        }
    }

    pub async fn flush(&self) {
        self.destroyed
            .store(true, std::sync::atomic::Ordering::Release);

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
