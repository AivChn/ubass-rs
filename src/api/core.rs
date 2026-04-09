use std::sync::LazyLock;

use futures::lock::Mutex;

use crate::{
    DEFAULT_PORT,
    manager::{self, AppId},
};

use super::error::ApiErrors;

#[derive(PartialEq)]
enum APIState {
    Open,
    Closed,
}

static API_STATE: LazyLock<Mutex<APIState>> = LazyLock::new(|| Mutex::new(APIState::Closed));

#[derive(Clone, Copy, Debug)]
struct _Private;

// TODO: replace this with full API state struct
#[derive(Clone, Debug)]
#[allow(private_bounds)]
pub struct Api(_Private);

impl Api {
    async fn new() -> Result<Self, ApiErrors> {
        let mut lock = API_STATE.lock().await;
        match *lock {
            APIState::Open => Err(ApiErrors::AlreadyOpen),
            APIState::Closed => {
                *lock = APIState::Open;
                Ok(Api(_Private))
            }
        }
    }
}

pub async fn open(app_id: String, port: Option<u16>) -> Result<Api, ApiErrors> {
    if *API_STATE.lock().await == APIState::Open {
        return Err(ApiErrors::AlreadyOpen);
    }

    let port = match port {
        Some(0..=1024) => return Err(ApiErrors::InvalidPort),
        Some(port) => port,
        None => DEFAULT_PORT,
    };

    manager::open(port, AppId::new(app_id))?;

    // TODO: finish constructing the API

    Api::new().await
}

// TODO: this API
impl Api {
    pub async fn close(&self) -> Result<(), ApiErrors> {
        let mut lock = API_STATE.lock().await;
        // TODO: manager close
        todo!("manager close");
        *lock = APIState::Closed;

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
        self.close();
    }
}
