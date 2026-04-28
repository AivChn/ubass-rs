#![feature(box_patterns)]
#![allow(unused)]
#![allow(dead_code)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::cast_lossless)]
#![warn(clippy::must_use_candidate)]
#![deny(clippy::async_yields_async)]
// #![deny(clippy::todo)]

pub mod api;
pub mod error;
pub mod manager;
pub mod packet_processor;
pub mod prelude;
pub mod transport;
pub mod utils;

pub const DEFAULT_PORT: u16 = 8455;
