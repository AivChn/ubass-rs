#![allow(async_fn_in_trait)]
#![allow(clippy::len_without_is_empty)]
use std::{fmt::Debug, ops::Range, ptr, slice::SliceIndex};

use tracing::debug;

use crate::{
    manager::packets::{BytePosition, MAX_PAYLOAD_LENGTH},
    o_unwrap_or_return,
    utils::{LogFail, PanicInDebug},
};

#[derive(Debug, Clone, Copy)]
pub struct Buffer(*mut [u8]);

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    pub fn new(ptr: *mut [u8]) -> Self {
        Self(ptr)
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
    /// Number of batch-level events (data `batch_end`s, FEC recoveries) since
    /// the last end-of-session retransmit sweep. Used by the receiver to
    /// re-issue the sweep when the session has stalled, without relying on a
    /// wall-clock timer.
    batches_since_sweep: u32,
}

impl From<&[u8]> for WriteableBuffer {
    fn from(value: &[u8]) -> Self {
        Self {
            // yes i dislike this too
            buffer: Buffer::new(std::ptr::from_ref(value).cast_mut()),
            head: BytePosition(0),
            map: vec![false; value.len() / MAX_PAYLOAD_LENGTH + 1],
            batches_since_sweep: 0,
        }
    }
}

impl<T> From<*mut T> for WriteableBuffer
where
    *mut T: Into<*mut [u8]>,
    T: ?Sized,
{
    fn from(value: *mut T) -> Self {
        let value = value.into();
        Self {
            buffer: Buffer::new(value),
            head: BytePosition(0),
            map: vec![false; value.len() / MAX_PAYLOAD_LENGTH + 1],
            batches_since_sweep: 0,
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

    /// True when the buffer is fully filled — head reached the end AND no chunk
    /// is still missing. Callers that are willing to declare done with holes
    /// (e.g. `complete_allow_partial`) should additionally check `head_at_end`.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.head_at_end() && self.map.iter().all(|&filled| filled)
    }

    /// True when `head` has reached the end of the buffer. May still have unfilled
    /// chunks below `head` if the stream involved seeks or losses.
    #[must_use]
    pub fn head_at_end(&self) -> bool {
        self.head() >= self.len()
    }

    /// Increment the "batches seen since last sweep" counter and return the
    /// new value. Called once per batch-level event (data `batch_end`, FEC
    /// recovery) while the receiver is in finalize state.
    pub fn note_batch_end(&mut self) -> u32 {
        self.batches_since_sweep = self.batches_since_sweep.saturating_add(1);
        self.batches_since_sweep
    }

    /// Reset the resweep counter. Called whenever a fresh end-of-session
    /// retransmit sweep is issued.
    pub fn reset_sweep_counter(&mut self) {
        self.batches_since_sweep = 0;
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn seek_head(&mut self, pos: BytePosition) -> Option<bool> {
        let prev = self.head;
        // sanity-check pos is in range
        self.position_to_index(pos)?;
        if pos > prev {
            // forward seek: jump head to pos so [prev, pos] becomes a detectable
            // hole region (find_holes only looks at [0, head]).
            // Align down to a chunk boundary so advance_head walks aligned positions —
            // server's ReadableBuffer::seek also floors to MAX_PAYLOAD_LENGTH, so this
            // keeps both sides in lockstep.
            let aligned = (*pos / MAX_PAYLOAD_LENGTH as u32) * MAX_PAYLOAD_LENGTH as u32;
            self.head = BytePosition(aligned);
            self.advance_head();
            Some(true)
        } else {
            // backward seek: don't retreat head — that would shrink the hole-detection
            // window and lose track of previously-requested gaps. Just signal "no forward
            // progress" so the caller skips sending a seek packet.
            Some(false)
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn last_occupied_pos(&self, from: BytePosition) -> Option<BytePosition> {
        let mut i = self.position_to_index(from)?;
        while let Some(prev) = i.checked_sub(1)
            && !self.map[prev]
        {
            i = prev;
        }

        Some(BytePosition((i * MAX_PAYLOAD_LENGTH) as u32))
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
        debug!("occupied position {i}");
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    // TODO:
    /// # Panics
    pub fn find_holes(&self, until: BytePosition) -> Vec<BytePosition> {
        // When the caller is asking up to or past end-of-buffer we want the full
        // map (including the trailing partial-chunk index). Otherwise stay below
        // head so we don't surface chunks the stream hasn't reached yet.
        let end_idx = if (*until as usize) >= self.len() {
            self.map.len()
        } else {
            self.position_to_index(until.min(self.head))
                .expect("head is always a valid position")
        };
        (0..end_idx)
            .filter_map(|i| (!self.map[i]).then_some(BytePosition((i * MAX_PAYLOAD_LENGTH) as u32)))
            .collect()
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn advance_head(&mut self) {
        let mut i = o_unwrap_or_return!(self.position_to_index(self.head));

        while i < self.map.len() && self.map[i] {
            *self.head += MAX_PAYLOAD_LENGTH as u32;
            i += 1;
        }

        if i == self.map.len() {
            *self.head = self.len() as u32;
        }

        debug!("advanced head to {}", self.head());
    }

    pub fn write(
        &mut self,
        position: BytePosition,
        to_write: impl AsRef<[u8]>,
    ) -> Option<Range<usize>> {
        let to_write = to_write.as_ref();
        if self
            .position_occupied(position)
            .log_warn("position not valid")?
            .log_warn("position occupied")
            || (to_write.len() != MAX_PAYLOAD_LENGTH
                && self.position_to_index(position)? != self.map.len() - 1)
                .log_warn("position isnt last but size isnt max")
            || (*position as usize + to_write.len() > self.buffer.len())
                .log_warn("buffer doesnt fit")
        {
            return None;
        }

        self.occupy(position);
        let position = *position as usize;
        let range = position..position + to_write.len();
        // TODO: make this error explicitly
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
    pub fn into_vec(self) -> Vec<u8> {
        self.buffer.into()
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.buffer.len()
    }

    #[must_use]
    pub const fn current_position(&self) -> usize {
        self.head
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn seek(&mut self, pos: BytePosition) -> BytePosition {
        let exact = (*pos as usize).min(self.len());
        let prev = self.head;
        self.head = exact - (exact % MAX_PAYLOAD_LENGTH);
        BytePosition(prev as u32)
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

pub trait PendingStream {
    type Stream: Stream;
    type Error: std::error::Error;
    type OwningConnection: Connection;

    async fn ready(
        self,
        connection: Self::OwningConnection,
    ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>;
    async fn discard(self) -> core::result::Result<(), Self::Error>;
}

pub trait RequestedStream: Sized {
    type Stream: Stream;
    type Error: std::error::Error;
    type OwningConnection: Connection;

    fn track_id(&self) -> &[u8];
    async fn reject(self, reason: impl Into<String>) -> Result<(), Self>;
    async fn approve_and_ready(
        self,
        connection: Self::OwningConnection,
    ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>;
    async fn approve_if_and_ready(
        self,
        f: impl FnOnce(&[u8]) -> bool,
        reject_reason: impl Into<String>,
        connection: Self::OwningConnection,
    ) -> Option<core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>>;
}

pub trait Stream {
    type Error: std::error::Error;
    type Idx: SliceIndex<[u8]>;
    type Connection: Connection;

    fn current_position(&self) -> Self::Idx;
    fn is_playing(&self) -> bool;
    async fn is_done(&self) -> bool;
    async fn complete(self) -> Result<Self::Connection, Self::Error>;
    async fn close(self) -> Result<Self::Connection, Self::Error>;
}

pub trait PlaybackControl: Stream {
    async fn play(&self) -> Result<Self::Idx, Self::Error>;
    async fn pause(&self) -> Result<Self::Idx, Self::Error>;
    async fn seek(&self, position: Self::Idx) -> Result<Self::Idx, Self::Error>;
}

pub trait IncomingConnection: Sized {
    type Connection: Connection;
    type Error: std::error::Error;

    fn app_id(&self) -> &str;
    async fn reject(self, reason: impl Into<String>) -> Result<(), Self>;
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
    async fn discard(self) -> core::result::Result<(), Self::Error>;
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
