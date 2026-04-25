#![allow(async_fn_in_trait)]
#![allow(clippy::len_without_is_empty)]
use std::{
    fmt::Debug,
    ops::{Range, Sub},
    slice::SliceIndex,
};

use tokio::sync::Notify;

use crate::manager::packets::MAX_PAYLOAD_LENGTH;

#[derive(Debug)]
pub struct WriteableBuffer<'buf> {
    buffer: &'buf mut [u8],
    head: usize,
}

impl AsRef<[u8]> for WriteableBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
        self.buffer
    }
}

impl<'buf, T> From<&'buf mut T> for WriteableBuffer<'buf>
where
    &'buf mut T: Into<&'buf mut [u8]>,
{
    fn from(value: &'buf mut T) -> Self {
        Self {
            buffer: value.into(),
            head: 0,
        }
    }
}

impl WriteableBuffer<'_> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub fn head(&self) -> usize {
        self.head
    }

    pub fn write(&mut self, to_write: impl AsRef<[u8]>) -> Option<Range<usize>> {
        let to_write = to_write.as_ref();
        if self.head + to_write.len() > self.len() {
            None
        } else {
            let head = self.head;
            self.head += to_write.len();
            let range = head..self.head;
            self.buffer[range.clone()].copy_from_slice(to_write);
            Some(range)
        }
    }
}

#[derive(Debug)]
pub struct ReadableBuffer {
    buffer: Box<[u8]>,
    current_position: usize,
}

impl ReadableBuffer {
    #[must_use]
    pub fn into_vec(self) -> Vec<u8> {
        self.buffer.into()
    }
}

impl<T: Into<Box<[u8]>>> From<T> for ReadableBuffer {
    fn from(value: T) -> Self {
        Self {
            buffer: value.into(),
            current_position: 0,
        }
    }
}

impl ReadableBuffer {
    #[must_use]
    pub const fn len(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub const fn current_position(&self) -> usize {
        self.current_position
    }

    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.buffer.len() <= self.current_position
    }

    pub fn next(&mut self, size: usize) -> Option<&[u8]> {
        if self.is_done() {
            None
        } else {
            let curr = self.current_position;
            self.current_position = self.buffer.len().min(self.current_position + size);
            Some(&self.buffer[curr..self.current_position])
        }
    }

    #[must_use]
    pub fn read(&self, range: Range<usize>) -> Option<&[u8]> {
        let end = range.end.min(self.buffer.len());
        self.buffer.get(range.start..end)
    }
}

pub trait Stream {
    type Error: std::error::Error;
    type Idx: SliceIndex<[u8]>;
    type Connection: Connection;

    async fn pause(&mut self) -> Result<Self::Idx, Self::Error>;
    async fn play(&mut self) -> Result<Self::Idx, Self::Error>;
    async fn seek(&mut self, position: Self::Idx) -> Result<Self::Idx, Self::Error>;
    fn current_position(&self) -> Self::Idx;
    fn is_playing(&self) -> bool;
    async fn is_done(&self) -> bool;
    async fn complete(self) -> Result<Self::Connection, Self::Error>;
}

pub trait IncomingConnection: Sized {
    type Connection: Connection;
    type Error: std::error::Error;

    fn app_id(&self) -> &str;
    async fn reject(self, reason: impl Into<String>) -> Result<(), Self>;
    async fn approve(&mut self) -> core::result::Result<(), Self::Error>;
    async fn ready(self) -> Option<core::result::Result<Self::Connection, Self::Error>>;
    async fn approve_and_ready(self) -> core::result::Result<Self::Connection, Self::Error>;
    async fn approve_if_and_ready(
        self,
        f: impl FnOnce(&str) -> bool,
        reject_reason: impl Into<String>,
    ) -> Option<core::result::Result<Self::Connection, Self::Error>>;
}

pub trait PendingConnection {
    type Connection: Connection;
    type Error: std::error::Error;

    async fn ready(self) -> core::result::Result<Self::Connection, Self::Error>;
}

pub trait Connection {
    type Event;
    type Error: std::error::Error;
    type InputStream: Stream;
    type OutputStream: Stream;

    async fn listen(&mut self) -> core::result::Result<Self::Event, Self::Error>;
    async fn send(
        self,
        buffer: impl Into<ReadableBuffer>,
    ) -> core::result::Result<Self::OutputStream, Self::Error>;
    async fn request(
        self,
        identifier: impl Into<Box<[u8]>>,
        buffer: impl Into<WriteableBuffer>,
    ) -> core::result::Result<Self::InputStream, Self::Error>;
    async fn close(self);
}
