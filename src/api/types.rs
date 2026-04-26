#![allow(async_fn_in_trait)]
#![allow(clippy::len_without_is_empty)]
use std::{
    collections::btree_map::VacantEntry,
    fmt::Debug,
    ops::{Range, Sub},
    ptr,
    slice::SliceIndex,
};

use derive_more::Deref;
use tokio::sync::Notify;

use crate::{
    manager::packets::{BytePosition, MAX_PAYLOAD_LENGTH},
    o_unwrap_or_return,
    utils::PanicInDebug,
};

#[derive(Debug, Clone, Copy)]
pub struct Buffer(*mut [u8]);

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    pub fn new(ptr: &mut [u8]) -> Self {
        Self(ptr::from_mut(ptr))
    }

    pub const fn len(self) -> usize {
        self.0.len()
    }

    pub unsafe fn as_mut(&mut self) -> Option<&mut [u8]> {
        unsafe { self.0.as_mut() }
    }
}

#[derive(Debug)]
pub struct WriteableBuffer {
    buffer: Buffer,
    head: BytePosition,
    map: Vec<bool>,
}

impl<'buf, T> From<&'buf mut T> for WriteableBuffer
where
    &'buf mut T: Into<&'buf mut [u8]>,
    T: ?Sized,
{
    fn from(value: &'buf mut T) -> Self {
        let value = value.into();
        Self {
            buffer: Buffer::new(value),
            head: BytePosition(0),
            map: vec![false; value.len() / MAX_PAYLOAD_LENGTH + 1],
        }
    }
}

impl WriteableBuffer {
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub fn head(&self) -> usize {
        *self.head as usize
    }

    const fn position_to_index(&self, position: BytePosition) -> Option<usize> {
        if self.buffer.len() < position.0 as usize {
            None
        } else {
            Some((position.0 as usize) / MAX_PAYLOAD_LENGTH)
        }
    }

    #[must_use]
    pub fn position_occupied(&self, position: BytePosition) -> Option<bool> {
        Some(self.map[self.position_to_index(position)?])
    }

    pub fn occupy(&mut self, position: BytePosition) {
        let i = o_unwrap_or_return!(self.position_to_index(position).panic_in_debug(&format!(
            "Invariant broken while trying to occupy a chunk \
                    for position {position}: position out of bounds"
        )));
        debug_assert!(
            !self.map[i],
            "Invariant broken while trying to occupy a chunk at index {i} (position {position}): chunk already occupied"
        );
        self.map[i] = true;
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn advance_head(&mut self) {
        let mut i =
            o_unwrap_or_return!(self.position_to_index(self.head).panic_in_debug(&format!(
                "Invariant broken in `advance_head`: head was not a valid index for the buffer \
                        (head: {}, buffer length: {})",
                self.head,
                self.len()
            )));

        while i < self.map.len() - 1 && self.map[i] {
            *self.head += MAX_PAYLOAD_LENGTH as u32;
            i += 1;
        }

        if i == self.map.len() - 1 {
            *self.head = self.len() as u32;
        }
    }

    pub fn write(
        &mut self,
        position: BytePosition,
        to_write: impl AsRef<[u8]>,
    ) -> Option<Range<usize>> {
        let to_write = to_write.as_ref();
        if self.position_occupied(position)?
            || (to_write.len() != MAX_PAYLOAD_LENGTH
                && self.position_to_index(position)? != self.map.len() - 1)
            || position.0 as usize + to_write.len() >= self.buffer.len()
        {
            return None;
        }

        self.occupy(position);
        let position = *position as usize;
        let range = position..position + to_write.len();
        let buf = unsafe { self.buffer.as_mut()? };
        buf[range.clone()].copy_from_slice(to_write);
        self.advance_head();
        Some(range)
    }
}

#[derive(Debug)]
pub struct ReadableBuffer {
    buffer: Box<[u8]>,
    head: usize,
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
            head: 0,
        }
    }
}

impl Iterator for ReadableBuffer {
    type Item = (BytePosition, Box<[u8]>);

    #[allow(clippy::cast_possible_truncation)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done() {
            None
        } else {
            let curr = self.head;
            self.head = self.buffer.len().min(self.head + MAX_PAYLOAD_LENGTH);
            Some((
                BytePosition(curr as u32),
                Box::from(&self.buffer[curr..self.head]),
            ))
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
        self.head
    }

    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.buffer.len() <= self.head
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
