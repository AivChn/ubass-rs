mod inbound;
mod outbound;
mod types;

pub use types::{
    AppEvent, Connection, ConnectionEvent, IncomingConnection, PendingConnection, PendingStream,
    RequestedStream,
};

use crate::{
    api::core::types::{ApiInner, InnerAppEvent},
    error::ApiErrors,
};

use std::sync::Arc;

use crate::{DEFAULT_PORT, manager::AppId};


pub struct Api {
    inner: Arc<ApiInner>,
}
impl Api {
    /// Opens the protocol, returning the Api singleton or an error if its already open.
    ///
    /// # Errors
    /// - `InvalidAppId` if the app ID is longer than [`MAX_PAYLOAD_LENGTH`] or not valid ASCII
    /// - `InvalidPort` if the port is 1024 or lower
    /// - `AlreadyOpen` if the protocol is already open
    pub fn open(app_id: impl Into<String>, port: Option<u16>) -> Result<Self, ApiErrors> {
        let app_id = app_id.into();
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
            InnerAppEvent::ProtocolFailed(reason) => Ok(AppEvent::ProtocolFailed(reason)),
        }
    }
}

const fn verify_app_id(app_id: &str) -> bool {
    app_id.is_ascii() && app_id.len() <= AppId::MAX_LENGTH
}
