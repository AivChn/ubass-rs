use std::sync::OnceLock;
use tokio::time::Instant;

pub use crate::error::*;
pub use crate::manager::packets;
pub use crate::utils::*;
pub use ubass_macros::*;

pub static PROTOCOL_EPOCH: OnceLock<Instant> = OnceLock::new();
