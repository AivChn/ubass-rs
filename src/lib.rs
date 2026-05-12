//#![allow(unused_imports)]
//#![allow(unused_variables)]
//#![allow(dead_code)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::cast_lossless)]
#![warn(clippy::must_use_candidate)]
#![deny(clippy::async_yields_async)]
#![warn(clippy::todo)]

pub mod api;
pub mod error;
pub mod manager;
pub mod packet_processor;
pub mod prelude;
pub mod transport;
pub mod utils;

pub use api::Api;

pub const DEFAULT_PORT: u16 = 8455;
