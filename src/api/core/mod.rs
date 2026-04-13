mod inbound;
mod outbound;

use crate::{
    lock_read, lock_write,
    manager::packets::{MAX_PAYLOAD_LENGTH, SessionId},
    prelude::{HashMap, Timestamp},
};

use std::{
    collections::VecDeque,
    ptr,
    sync::{LazyLock, atomic::AtomicBool},
    thread::JoinHandle,
};

use rand::random;
use tokio::sync::RwLock;

use crate::{
    DEFAULT_PORT,
    manager::{self, AppId},
};

use super::error::ApiErrors;

static PROTOCOL_OPEN: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
pub struct Api {
    manager_handle: JoinHandle<core::result::Result<(), ApiErrors>>,
}

impl Api {
    fn new(port: u16, app_id: AppId) -> Result<Self, ApiErrors> {
        if !PROTOCOL_OPEN.swap(true, std::sync::atomic::Ordering::Relaxed) {
            Ok(Api {
                manager_handle: manager::open(port, app_id)?,
            })
        } else {
            Err(ApiErrors::AlreadyOpen)
        }
    }
}

pub async fn open(app_id: String, port: Option<u16>) -> Result<Api, ApiErrors> {
    let port = match port {
        Some(0..=1024) => return Err(ApiErrors::InvalidPort),
        Some(port) => port,
        None => DEFAULT_PORT,
    };

    if !app_id.is_ascii() || app_id.len() > AppId::MAX_LENGTH {
        return Err(ApiErrors::InvalidAppId);
    }

    Api::new(port, AppId::new(app_id))
}

// TODO: this API
impl Api {
    pub fn close(self) -> Result<(), ApiErrors> {
        PROTOCOL_OPEN.store(false, std::sync::atomic::Ordering::Relaxed);
        // TODO: manager close
        todo!("manager close");

        Ok(())
    }

    pub fn connect() -> Result<(), ApiErrors> {
        todo!()
    }

    pub fn listen() -> Result<(), ApiErrors> {
        todo!()
    }

    pub fn request_track() -> Result<(), ApiErrors> {
        todo!()
    }
}

impl Drop for Api {
    fn drop(&mut self) {
        let Err(res) = (unsafe { ptr::read(self).close() }) else {
            return;
        };
        dbg!(res);
    }
}

#[derive(Clone)]
enum RequestPayload {
    AppId(AppId),
    TrackRequest(SessionId, Box<[u8; MAX_PAYLOAD_LENGTH]>),
}

struct AppRequestsMap {
    map: RwLock<HashMap<u32, RequestPayload>>,
    queue: RwLock<VecDeque<(Timestamp, u32)>>,
}

impl Default for AppRequestsMap {
    fn default() -> Self {
        Self {
            map: RwLock::default(),
            queue: RwLock::new(VecDeque::with_capacity(32)),
        }
    }
}

impl AppRequestsMap {
    const PRUNE_INTERVAL: u64 = 10_000;

    async fn add(&self, payload: &RequestPayload) -> u32 {
        let mut map = lock_write!(self.map);
        let key = loop {
            let tmp = random::<u32>();
            if !map.contains_key(&tmp) {
                break tmp;
            }
        };

        map.insert(key, payload.clone());
        drop(map);
        lock_write!(self.queue).push_back((Timestamp::now(), key));

        key
    }

    async fn contains(&self, key: u32) -> bool {
        lock_read!(self.map).contains_key(&key)
    }

    async fn prune(&self) {
        let mut queue = lock_write!(self.queue);
        let to_remove = queue
            .iter()
            .take_while(|(ts, k)| ts.been_longer_than(Self::PRUNE_INTERVAL))
            .count();

        let to_remove: Vec<_> = queue.drain(..to_remove).map(|(ts, k)| k).collect();
        drop(queue);

        let mut map = lock_write!(self.map);
        for k in to_remove {
            map.remove(&k);
        }
    }
}
