#![allow(async_fn_in_trait)]
#![allow(clippy::len_without_is_empty)]
use std::{cmp::Ordering, fmt::Debug, ops::Range, slice::SliceIndex};

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
pub struct Area(Range<usize>);

impl Area {
    // Tunables for the score model. Picked as starting values; expected to be
    // retuned once the policy runs against real traffic.
    const URGENCY_DECAY_CHUNKS: f64 = 4.0;
    const BEFORE_HEAD_SIZE_DENOM: f64 = 8.0;
    const BEFORE_HEAD_URGENCY_CAP: f64 = 0.9;
    const CONFIDENCE_HALF_SAT_CHUNKS: f64 = 4.0;

    fn update_end(&mut self, end: usize) {
        self.0.end = end;
    }

    fn update_start(&mut self, start: usize) {
        self.0.start = start;
    }

    pub fn range(&self) -> &Range<usize> {
        &self.0
    }

    // Urgency: how much we want this hole filled, ignoring whether it's
    // actually missing. Head-adjacent after-head holes peak at 1; before-head
    // holes are capped strictly below 1 so live progress always wins on
    // urgency alone.
    #[allow(clippy::cast_precision_loss)]
    fn urgency(&self, head_chunk: usize) -> f64 {
        if self.0.start >= head_chunk {
            let distance = (self.0.start - head_chunk) as f64;
            1.0 / (1.0 + distance / Self::URGENCY_DECAY_CHUNKS)
        } else {
            let size = (self.0.end - self.0.start) as f64;
            (size / Self::BEFORE_HEAD_SIZE_DENOM).min(Self::BEFORE_HEAD_URGENCY_CAP)
        }
    }

    // Confidence-lost: probability the hole is real loss vs. still in flight.
    // Proxy is the count of valid chunks that arrived past this hole — more
    // later arrivals → it's been overtaken → more likely actually lost.
    #[allow(clippy::cast_precision_loss)]
    fn confidence_lost(&self, total_chunks: usize, later_invalid: usize) -> f64 {
        let valid_past = total_chunks
            .saturating_sub(self.0.end)
            .saturating_sub(later_invalid) as f64;
        valid_past / (valid_past + Self::CONFIDENCE_HALF_SAT_CHUNKS)
    }
}

#[derive(Debug)]
pub struct WriteableBuffer {
    buffer: Buffer,
    head: BytePosition,
    map: Vec<bool>,
    invalid_areas: Vec<Area>,
}

impl From<&[u8]> for WriteableBuffer {
    fn from(value: &[u8]) -> Self {
        let num_chunks = value.len() / MAX_PAYLOAD_LENGTH + 1;
        Self {
            buffer: Buffer::new(std::ptr::from_ref(value).cast_mut()),
            head: BytePosition(0),
            map: vec![false; num_chunks],
            invalid_areas: vec![Area(Range {
                start: 0,
                end: num_chunks,
            })],
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
        let num_chunks = value.len() / MAX_PAYLOAD_LENGTH + 1;
        Self {
            buffer: Buffer::new(value),
            head: BytePosition(0),
            map: vec![false; num_chunks],
            invalid_areas: vec![Area(Range {
                start: 0,
                end: num_chunks,
            })],
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

        if let Some(pos) = self.invalid_areas.iter().position(|a| a.0.contains(&i)) {
            let (start, end) = (
                self.invalid_areas[pos].0.start,
                self.invalid_areas[pos].0.end,
            );

            if start == i {
                if end == i + 1 {
                    self.invalid_areas.remove(pos);
                } else {
                    self.invalid_areas[pos].update_start(i + 1);
                }
            } else {
                self.invalid_areas[pos].update_end(i);
                if end - 1 != i {
                    self.invalid_areas
                        .insert(pos + 1, Area(Range { start: i + 1, end }));
                }
            }
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    // TODO:
    /// # Panics
    pub fn find_holes(&self, until: BytePosition) -> Vec<BytePosition> {
        let until = until.min(BytePosition::from(self.len()));
        let end_idx = self.position_to_index(until).expect("is always in range");

        (0..end_idx)
            .filter_map(|i| (!self.map[i]).then_some(BytePosition((i * MAX_PAYLOAD_LENGTH) as u32)))
            .collect()
    }

    // Score below which we don't issue a retransmit request. With the current
    // shape consts, this lets head-adjacent or strong-confidence holes through
    // and filters out the noisy "moderate urgency, moderate confidence" middle.
    pub const SCORE_THRESHOLD: f64 = 0.3;

    // Maximum number of holes to ask for in a single policy tick. Avoids
    // flooding the sender when fragmentation is high.
    pub const MAX_REQUESTS_PER_TICK: usize = 2;

    // Score = urgency * confidence_lost, evaluated for every tracked hole.
    // Returns areas paired with their score in the original (sorted-by-start)
    // order; callers pick which to act on (e.g. top-N above threshold).
    #[must_use]
    pub fn score_areas(&self) -> Vec<(&Area, f64)> {
        let head_chunk = (*self.head as usize) / MAX_PAYLOAD_LENGTH;
        let total_chunks = self.map.len();

        // suffix_invalid[i] = sum of lengths of invalid_areas[i..]
        // valid_past for area at index i = total_chunks - area.end - suffix_invalid[i + 1]
        let mut suffix_invalid = vec![0usize; self.invalid_areas.len() + 1];
        for (i, area) in self.invalid_areas.iter().enumerate().rev() {
            suffix_invalid[i] = suffix_invalid[i + 1] + (area.0.end - area.0.start);
        }

        self.invalid_areas
            .iter()
            .enumerate()
            .map(|(i, area)| {
                let urg = area.urgency(head_chunk);
                let conf = area.confidence_lost(total_chunks, suffix_invalid[i + 1]);
                (area, urg * conf)
            })
            .collect()
    }

    // Filter `score_areas` by `SCORE_THRESHOLD`, sort descending by score, and
    // truncate to `MAX_REQUESTS_PER_TICK`. The returned slice is the policy's
    // current ask list.
    #[must_use]
    pub fn requestable_areas(&self) -> Vec<&Area> {
        let mut scored = self.score_areas();
        scored.retain(|(_, s)| *s >= Self::SCORE_THRESHOLD);
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scored.truncate(Self::MAX_REQUESTS_PER_TICK);
        scored.into_iter().map(|(a, _)| a).collect()
    }

    // Every currently-invalid area, no scoring / threshold / per-tick cap.
    // For end-of-stream cleanup: once `head` has reached `len`, every
    // remaining hole is confirmed loss and must be filled. Pending dedup
    // and sender-side per-tick pacing keep this from flooding.
    #[must_use]
    pub fn all_invalid_areas(&self) -> Vec<&Area> {
        self.invalid_areas.iter().collect()
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

/// Sealed marker trait carrying the direction of a stream — receiver-side
/// (`Input`) or sender-side (`Output`). Used to parametrize `Stream`,
/// `RequestedStream`, and `PendingStream` so the same struct shape works
/// in either role and direction-specific behavior can be specialized via
/// trait impls (`impl Stream for Stream<Input>`, etc.).
pub trait StreamDirection: stream_direction_sealed::Sealed {}

mod stream_direction_sealed {
    pub trait Sealed {}
}

#[derive(Debug)]
pub struct Input;
#[derive(Debug)]
pub struct Output;
impl stream_direction_sealed::Sealed for Input {}
impl stream_direction_sealed::Sealed for Output {}
impl StreamDirection for Input {}
impl StreamDirection for Output {}

pub trait PendingStream: Sized {
    type Stream: Stream;
    type Error: std::error::Error;
    type OwningConnection: Connection;

    async fn ready(
        self,
    ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>;
    async fn discard(
        self,
    ) -> core::result::Result<Self::OwningConnection, (Self::Error, Self::OwningConnection)>;
}

pub enum ApprovalStatus<A, S> {
    Approved(A),
    Rejected(S),
}

pub trait RequestedStream: Sized {
    type Stream: Stream;
    type Error: std::error::Error;
    type OwningConnection: Connection;
    type ApprovalBuffer;

    fn track_id(&self) -> &[u8];

    async fn reject(self) -> core::result::Result<Self::OwningConnection, Self::Error>;
    async fn approve_and_ready(
        self,
        buffer: impl Into<Self::ApprovalBuffer>,
    ) -> core::result::Result<Self::Stream, (Self::Error, Self::OwningConnection)>;

    async fn approve_if_and_ready(
        self,
        f: impl FnOnce(&[u8]) -> bool,
        buffer: impl Into<Self::ApprovalBuffer>,
    ) -> ApprovalStatus<
        Result<Self::Stream, (Self::Error, Self::OwningConnection)>,
        Result<Self::OwningConnection, Self::Error>,
    >
    where
        Self: Sized,
    {
        {
            if f(self.track_id()) {
                ApprovalStatus::Approved(self.approve_and_ready(buffer).await)
            } else {
                ApprovalStatus::Rejected(self.reject().await)
            }
        }
    }
}

pub trait Stream {
    type Error: std::error::Error;
    type Idx: SliceIndex<[u8]>;
    type Connection: Connection;

    fn current_position(&self) -> Self::Idx;
    fn is_playing(&self) -> bool;
    async fn is_done(&self) -> bool;
    async fn complete(self) -> Result<Self::Connection, Self::Error>;
    async fn close(self) -> Result<Self::Connection, (Self::Error, Self::Connection)>;
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

pub trait Connection: Sized {
    /// Event yielded by `listen`. Listen consumes the connection because
    /// some events (e.g. `TrackRequested`) wrap a `RequestedStream` that
    /// owns the connection; terminal events (closed) drop it.
    type Event;
    type Error: std::error::Error;
    type InputStream: Stream;
    type OutputStream: Stream;
    /// Pending input stream returned by `request` — owns the connection
    /// until `ready()` finalizes into an `InputStream`.
    type PendingInputStream: PendingStream<Stream = Self::InputStream, OwningConnection = Self>;

    async fn listen(self) -> core::result::Result<Self::Event, Self::Error>;
    async fn send(
        self,
        buffer: impl Into<ReadableBuffer>,
    ) -> core::result::Result<Self::OutputStream, (Self::Error, Self)>;
    async fn request(
        self,
        identifier: impl Into<Box<[u8]>>,
        buffer: impl Into<WriteableBuffer>,
    ) -> core::result::Result<Self::PendingInputStream, (Self::Error, Self)>;
    async fn close(self);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a buffer with exactly `num_chunks` entries in `map`. The init
    // formula is `len / MAX_PAYLOAD_LENGTH + 1`, so `(n - 1) * MPL + 1` yields n.
    // Memory is leaked since the buffer holds a raw pointer; fine for unit tests.
    fn make_buf(num_chunks: usize) -> WriteableBuffer {
        let len = (num_chunks - 1) * MAX_PAYLOAD_LENGTH + 1;
        let leaked: &'static mut [u8] = vec![0u8; len].leak();
        WriteableBuffer::from(std::ptr::from_mut(leaked))
    }

    #[allow(clippy::cast_possible_truncation)]
    fn occupy_chunk(buf: &mut WriteableBuffer, idx: usize) {
        buf.occupy(BytePosition((idx * MAX_PAYLOAD_LENGTH) as u32));
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn fresh_buffer_scores_zero() {
        // One area covering the whole buffer, nothing valid past it → confidence 0.
        let buf = make_buf(8);
        let scores = buf.score_areas();
        assert_eq!(scores.len(), 1);
        // float_cmp allowed here because the score is expected to be exactly 0
        assert_eq!(scores[0].1, 0.0);
    }

    #[test]
    fn confidence_grows_as_chunks_arrive_past_hole() {
        // Hole [0, 4), valid [4, 8). Head still at 0, urgency=1.
        // valid_past = 8 - 4 - 0 = 4 → confidence = 4 / (4 + 4) = 0.5
        let mut buf = make_buf(8);
        for i in 4..8 {
            occupy_chunk(&mut buf, i);
        }
        let scores = buf.score_areas();
        assert_eq!(scores.len(), 1);
        assert!((scores[0].1 - 0.5).abs() < 1e-9);
    }

    #[test]
    fn head_adjacent_outranks_far_hole() {
        // Layout: hole [0,2), valid [2,4), hole [4,6), valid [6,10)
        let mut buf = make_buf(10);
        for i in [2, 3, 6, 7, 8, 9] {
            occupy_chunk(&mut buf, i);
        }
        let scores = buf.score_areas();
        assert_eq!(scores.len(), 2);
        assert!(
            scores[0].1 > scores[1].1,
            "head-adjacent {} should outrank far {}",
            scores[0].1,
            scores[1].1
        );
    }

    #[test]
    fn urgency_decays_continuously_with_distance() {
        // Property of `urgency` itself: monotonically decreasing as distance grows.
        let head_chunk = 0;
        let a0 = Area(0..1);
        let a1 = Area(2..3);
        let a2 = Area(8..9);
        let u0 = a0.urgency(head_chunk);
        let u1 = a1.urgency(head_chunk);
        let u2 = a2.urgency(head_chunk);
        assert!(u0 > u1 && u1 > u2);
        assert!((u0 - 1.0).abs() < 1e-9);
    }

    #[test]
    fn before_head_urgency_capped_below_one() {
        // Area with start < head_chunk classified as before-head; cap kicks in.
        let head_chunk = 5;
        let small = Area(0..2); // size 2 → 2/8 = 0.25, no cap needed
        let huge = Area(0..100); // size 100 → 100/8 = 12.5, capped
        assert!((small.urgency(head_chunk) - 0.25).abs() < 1e-9);
        assert!((huge.urgency(head_chunk) - Area::BEFORE_HEAD_URGENCY_CAP).abs() < 1e-9);
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn before_head_score_zero_until_data_arrives_past() {
        // Forward-seek to chunk 10 in a 20-chunk buffer; nothing past head is
        // filled yet → confidence 0 → score 0, even though urgency is capped at 0.9.
        let mut buf = make_buf(20);
        #[allow(clippy::cast_possible_truncation)] // 10 * MAX_PAYLOAD_LENGTH = 13840 < u32::MAX
        buf.seek_head(BytePosition((10 * MAX_PAYLOAD_LENGTH) as u32));
        let scores = buf.score_areas();
        assert_eq!(scores.len(), 1);
        // float_cmp allowed here because the score is expected to be exactly 0
        assert_eq!(scores[0].1, 0.0);

        // Now fill a couple of chunks past head. Before-head area's score
        // becomes nonzero but stays at-or-below the urgency cap.
        occupy_chunk(&mut buf, 15);
        occupy_chunk(&mut buf, 16);
        let scores = buf.score_areas();
        let first = scores
            .iter()
            .find(|(a, _)| a.0.start == 0)
            .expect("[0, ...) area still present");
        assert!(first.1 > 0.0);
        assert!(first.1 <= Area::BEFORE_HEAD_URGENCY_CAP + 1e-9);
    }

    #[test]
    fn confidence_monotone_in_valid_past() {
        // Property of `confidence_lost` itself.
        let area = Area(0..2);
        let total = 100;
        let c_low = area.confidence_lost(total, /*later_invalid*/ 90);
        let c_mid = area.confidence_lost(total, 50);
        let c_high = area.confidence_lost(total, 0);
        assert!(c_low < c_mid && c_mid < c_high);
        assert!(c_high < 1.0);
    }

    #[test]
    fn requestable_areas_filters_below_threshold() {
        // Layout giving one strongly-scoring area and one weak one.
        // hole [0,1), valid [1,8): area 0 → urg=1, valid_past=7, conf=7/11≈0.636
        // → score ≈ 0.636 (above 0.3)
        // No other holes — so only one area total. Use a buffer with a single
        // strong hole to verify the threshold filter accepts it.
        let mut buf = make_buf(8);
        for i in 1..8 {
            occupy_chunk(&mut buf, i);
        }
        let picked = buf.requestable_areas();
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].0.start, 0);

        // Now a fresh buffer where every area scores 0 → nothing requestable.
        let buf = make_buf(8);
        assert!(buf.requestable_areas().is_empty());
    }

    #[test]
    fn requestable_areas_caps_at_max_and_returns_top_by_score() {
        // Three single-chunk holes at increasing distances from head with a lot
        // of valid data piled past the last one, so all clear the threshold but
        // urgency strictly decreases with distance.
        // Layout: hole [0,1), valid [1,2), hole [2,3), valid [3,4),
        //         hole [4,5), valid [5,16)
        let mut buf = make_buf(16);
        for i in [1, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] {
            occupy_chunk(&mut buf, i);
        }

        // Sanity: confirm three areas survived and all score above threshold.
        let scored = buf.score_areas();
        assert_eq!(scored.len(), 3);
        for (_, s) in &scored {
            assert!(*s >= WriteableBuffer::SCORE_THRESHOLD);
        }

        let picked = buf.requestable_areas();
        assert_eq!(picked.len(), WriteableBuffer::MAX_REQUESTS_PER_TICK);
        // Top two by urgency (distance) are the head-adjacent and the next.
        assert_eq!(picked[0].0.start, 0);
        assert_eq!(picked[1].0.start, 2);
    }
}
