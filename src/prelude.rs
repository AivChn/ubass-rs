use std::sync::OnceLock;
use tokio::time::Instant;

pub use crate::error::*;
pub use crate::manager::{packets, types::Timestamp};
pub use crate::packet_processor::serialize::Serialize;
pub use crate::utils::*;
pub use ubass_macros::*;

pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;

pub static PROTOCOL_EPOCH: OnceLock<Instant> = OnceLock::new();
