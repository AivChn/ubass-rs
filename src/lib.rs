#![allow(unused)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::must_use_candidate)]
#![warn(clippy::async_yields_async)]
#![deny(clippy::todo)]

pub mod api;
pub mod error;
pub mod manager;
pub mod packet_processor;
pub mod prelude;
pub mod transport;
pub mod utils;

pub const DEFAULT_PORT: u16 = 8455;
