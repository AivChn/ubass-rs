//#![allow(unused_imports)]
//#![allow(unused_variables)]
//#![allow(dead_code)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::cast_lossless)]
#![warn(clippy::must_use_candidate)]
#![deny(clippy::async_yields_async)]
#![warn(clippy::todo)]

pub mod api;

pub(crate) mod error;
pub(crate) mod manager;
pub(crate) mod packet_processor;
pub(crate) mod prelude;
pub(crate) mod transport;
pub(crate) mod utils;

pub use api::*;

pub const DEFAULT_PORT: u16 = 8455;
