use std::{
    any::Any,
    fmt::{Debug, format},
    net::SocketAddr,
    ops::Deref,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use async_trait::async_trait;
use futures::future::OptionFuture;
use tokio::{
    sync::{Mutex, Notify, RwLock},
    task::JoinHandle,
};

pub mod messages;

pub use messages::*;
use tracing::{error, info, warn};

#[macro_export]
macro_rules! debug_match_or_return {
    ($value:expr, $p:pat => $i:ident, $msg:expr) => {
        match $value {
            $p => $i,
            _ => {
                debug_assert!(false, "Invariant broken: {}", $msg);

                return;
            }
        }
    };
}

#[macro_export]
macro_rules! match_or_return {
    ($value:expr, $p:pat => $i:ident) => {
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

#[derive(Debug, Default)]
pub struct Shared<T: Send + Sync> {
    value: RwLock<T>,
    signal: Notify,
}

impl<T: Send + Sync> Shared<T> {
    pub fn new(value: T) -> Self {
        Self {
            value: RwLock::new(value),
            signal: Notify::default(),
        }
    }

    pub async fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let val = lock_read!(self.value);
        f(&*val)
    }

    pub async fn with_async<R>(&self, f: impl AsyncFnOnce(&T) -> R) -> R {
        let val = lock_read!(self.value);
        f(&*val).await
    }

    pub async fn listen_then<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        self.signal.notified().await;
        let mut val = lock_write!(self.value);
        f(&mut *val)
    }

    pub async fn listen(&self) {
        self.signal.notified().await;
    }

    pub async fn update(&self, value: T) {
        let mut val = lock_write!(self.value);
        *val = value;
        self.signal.notify_waiters();
    }

    pub async fn update_with(&self, f: impl Fn(&mut T)) {
        let mut val = lock_write!(self.value);
        f(&mut *val);
        self.signal.notify_waiters();
    }
}

impl<T: Send + Sync + Clone> Shared<T> {
    pub async fn read_cloned(&self) -> T {
        lock_read!(self.value).clone()
    }
}

impl<T: Send + Sync + Clone + Copy> Shared<T> {
    pub async fn read(&self) -> T {
        *lock_read!(self.value)
    }

    pub async fn get_next(&self) -> T {
        self.signal.notified().await;
        *lock_read!(self.value)
    }
}

pub trait PanicInDebug {
    #[must_use]
    fn panic_in_debug(self, msg: &str) -> Self;
}

impl<T, E: Debug> PanicInDebug for Result<T, E> {
    #[inline]
    fn panic_in_debug(self, msg: &str) -> Self {
        #[cfg(debug_assertions)]
        if let Err(e) = self {
            panic!("{msg}: {e:?}");
        }

        self
    }
}

impl<T> PanicInDebug for Option<T> {
    #[inline]
    fn panic_in_debug(self, msg: &str) -> Self {
        debug_assert!(self.is_some(), "{msg}");

        self
    }
}

pub trait LogFail {
    #[must_use]
    fn log_warn(self, msg: &str) -> Self;

    #[must_use]
    fn log_error(self, msg: &str) -> Self;

    #[must_use]
    fn log_info(self, msg: &str) -> Self;
}

impl LogFail for bool {
    fn log_warn(self, msg: &str) -> Self {
        if self {
            warn!("{msg}");
        }

        self
    }

    fn log_error(self, msg: &str) -> Self {
        if self {
            error!("{msg}");
        }

        self
    }

    fn log_info(self, msg: &str) -> Self {
        if self {
            info!("{msg}");
        }

        self
    }
}

impl<T> LogFail for Option<T> {
    #[inline]
    fn log_warn(self, msg: &str) -> Self {
        if self.is_none() {
            warn!("{}", msg);
        }

        self
    }

    #[inline]
    fn log_error(self, msg: &str) -> Self {
        if self.is_none() {
            error!("{}", msg);
        }

        self
    }

    #[inline]
    fn log_info(self, msg: &str) -> Self {
        if self.is_none() {
            info!("{}", msg);
        }

        self
    }
}

impl<T, E: Debug> LogFail for Result<T, E> {
    #[inline]
    fn log_warn(self, msg: &str) -> Self {
        if let Err(ref e) = self {
            warn!("{}: {:?}", msg, e);
        }

        self
    }

    #[inline]
    fn log_error(self, msg: &str) -> Self {
        if let Err(ref e) = self {
            error!("{}: {:?}", msg, e);
        }

        self
    }

    #[inline]
    fn log_info(self, msg: &str) -> Self {
        if let Err(ref e) = self {
            info!("{}: {:?}", msg, e);
        }

        self
    }
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
    pub fn dispatch<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let copy = self.clone();
        tokio::spawn(async move { copy.handles.lock().await.push(tokio::spawn(future)) });
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
