mod inbound;
mod outbound;
mod types;

pub use types::{AppEvent, Connection, IncomingConnection, PendingConnection};

use crate::{
    api::{
        ReadableBuffer,
        core::types::{ApiInner, InnerAppEvent},
    },
    error::ApiErrors,
    lock_read, lock_write,
    manager::packets::{MAX_PAYLOAD_LENGTH, PayloadField, SessionId},
    o_unwrap_or_return,
    prelude::{HashMap, Timestamp},
    r_unwrap_or_return,
    utils::ResponseReceiver,
};

use std::{
    sync::{Arc, Weak, atomic::AtomicBool},
    thread::JoinHandle,
};

use rand::random;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

use crate::{
    DEFAULT_PORT,
    manager::{self, AppId},
};

use crate::utils::{ApiCommand, ApiMessage, AppResponse, OneShot, SendDataRequest, SendTarget};
use types::{ApiFromManager, ApiToManager};

pub struct Api {
    inner: Arc<ApiInner>,
}

/// Opens the protocol, returning the Api singleton or an error if its already open.
///
/// # Errors
/// - `InvalidAppId` if the app ID is longer than [`MAX_PAYLOAD_LENGTH`] or not valid ASCII
/// - `InvalidPort` if the port is 1024 or lower
/// - `AlreadyOpen` if the protocol is already open
pub async fn open(app_id: String, port: Option<u16>) -> Result<Api, ApiErrors> {
    let port = match port {
        Some(0..=1024) => return Err(ApiErrors::InvalidPort),
        Some(port) => port,
        None => DEFAULT_PORT,
    };

    if !verify_app_id(&app_id) {
        return Err(ApiErrors::InvalidAppId);
    }

    Api::new(port, AppId::new(app_id))
}

impl Drop for Api {
    fn drop(&mut self) {
        let inner = unsafe { std::ptr::read(&raw const self.inner) };
        drop(inner);
    }
}

impl Api {
    fn new(port: u16, app_id: AppId) -> Result<Self, ApiErrors> {
        Ok(Self {
            inner: Arc::new(ApiInner::new(port, app_id)?),
        })
    }

    /// connects to the given address. This function will return a `PendingConnection` struct that
    /// can be awaited to get a ready connection at any point.
    ///
    /// # Errors
    /// This function will return an error if anything in the handshake process failed
    pub async fn connect(
        &self,
        addr: std::net::SocketAddr,
    ) -> core::result::Result<PendingConnection, ApiErrors> {
        let reply = self.inner.connect(addr).await?;
        Ok(PendingConnection::new(
            Arc::downgrade(&self.inner),
            addr,
            reply,
        ))
    }

    /// listens on the protocol for any message not attached to a session
    ///
    /// # Errors
    // TODO:
    pub async fn listen(&self) -> core::result::Result<AppEvent, ApiErrors> {
        match self.inner.listen().await? {
            InnerAppEvent::DataReceived { session_id, data } => {
                Ok(AppEvent::DataReceived { session_id, data })
            }
            InnerAppEvent::IncomingConnection {
                request,
                response,
                peer_address,
            } => {
                let incoming = IncomingConnection::new(
                    Arc::downgrade(&self.inner),
                    peer_address,
                    request.data,
                    request.response,
                    response,
                );

                Ok(AppEvent::IncomingConnection(incoming))
            }
            InnerAppEvent::Closed => Ok(AppEvent::Closed),
        }
    }

    /// # Errors
    // TODO:
    pub async fn send_data(
        &self,
        target: SendTarget,
        buffer: impl Into<ReadableBuffer>,
    ) -> core::result::Result<SessionId, ApiErrors> {
        self.inner.send_data(target, buffer.into()).await
    }

    /// # Errors
    // TODO:
    pub async fn approve(
        &self,
        id: u32,
        approved: bool,
        reason: String,
    ) -> core::result::Result<(), ApiErrors> {
        self.inner.approve(id, approved, reason).await
    }
}

const fn verify_app_id(app_id: &str) -> bool {
    app_id.is_ascii() && app_id.len() <= AppId::MAX_LENGTH
}
