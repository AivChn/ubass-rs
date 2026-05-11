use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use tokio::sync::{Notify, RwLock};

pub mod messages;

pub use messages::*;
use tracing::{debug, error, info, warn};

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
    ($value:expr, $p:pat) => {
        let $p = $value else {
            return;
        };
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
        self.signal.notify_one();
    }

    pub async fn update_with(&self, f: impl Fn(&mut T)) {
        let mut val = lock_write!(self.value);
        f(&mut *val);
        self.signal.notify_one();
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

    #[must_use]
    fn log_debug(self, msg: &str) -> Self;
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

    fn log_debug(self, msg: &str) -> Self {
        if self {
            debug!("{msg}");
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

    fn log_debug(self, msg: &str) -> Self {
        if self.is_none() {
            debug!("{msg}");
        }

        self
    }
}

impl<T, E: Debug> LogFail for Result<T, E> {
    #[inline]
    fn log_warn(self, msg: &str) -> Self {
        if let Err(e) = &self {
            warn!("{}: {:?}", msg, e);
        }

        self
    }

    #[inline]
    fn log_error(self, msg: &str) -> Self {
        if let Err(e) = &self {
            error!("{}: {:?}", msg, e);
        }

        self
    }

    #[inline]
    fn log_info(self, msg: &str) -> Self {
        if let Err(e) = &self {
            info!("{}: {:?}", msg, e);
        }

        self
    }

    fn log_debug(self, msg: &str) -> Self {
        if let Err(e) = &self {
            debug!("{}: {:?}", msg, e);
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

#[must_use]
pub fn not(b: bool) -> bool {
    !b
}

pub struct HandleMonitor {
    running: AtomicUsize,
    notify: Notify,
}

impl HandleMonitor {
    pub fn size(&self) -> usize {
        self.running.load(Ordering::Acquire)
    }

    #[inline]
    pub fn dispatch<F>(self: &Arc<Self>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.running.fetch_add(1, Ordering::Relaxed);
        let copy = self.clone();
        tokio::spawn(async move {
            future.await;
            if copy.running.fetch_sub(1, Ordering::AcqRel) == 1 {
                copy.notify.notify_one();
            }
        });
    }

    pub async fn flush(&self) {
        loop {
            if self.running.load(Ordering::Acquire) == 0 {
                return;
            }
            self.notify.notified().await;
        }
    }
}

impl Default for HandleMonitor {
    fn default() -> Self {
        Self {
            running: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }
}
