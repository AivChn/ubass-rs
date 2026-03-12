use std::sync::OnceLock;

pub use crate::error::PipeDirection::*;
pub use crate::error::*;
pub use crate::utils::*;
use tokio::time::Instant;
pub use ubass_macros::*;

pub static PROTOCOL_EPOCH: OnceLock<Instant> = OnceLock::new();
