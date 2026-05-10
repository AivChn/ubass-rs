# Bug-fix session summary

This document covers what was found, what was changed, and why, across the test-driven debugging session that started from the (then-hanging) ignored e2e test `audio_data_with_playback_control`.

## Test-suite state going in

- 47 unit / packet-processor / macro / transport tests: all passing.
- 6 / 8 `tests/e2e_test.rs` tests passing.
- `audio_data` (ignored): passing.
- `audio_data_with_playback_control` (ignored): **hung 2 min then panicked at `LONG_TIMEOUT`**.

The hang was the entry point.

## Bug 1 — Forward seek left a permanent unrecovered gap

**Where:** `src/api/types.rs::WriteableBuffer::seek_head`.

**Symptom:** After a forward seek to a position past the receiver's `head`, the server jumped ahead but the receiver's `head` didn't follow. Holes in `[old_head, seek_pos]` were invisible to retransmit detection (`find_holes` was clamped to `[0, head]`), so the receiver waited forever for data the server wasn't going to send.

**Fix:**
- Forward seek: jump `head` to `pos` directly (aligned down to a chunk boundary, matching `ReadableBuffer::seek` on the sender side which floors to `MAX_PAYLOAD_LENGTH`).
- Backward seek: don't retreat `head` — that would shrink the hole-detection window and lose track of previously-requested gaps; just signal "no forward progress" so the caller skips sending a seek packet.
- Out-of-range `pos`: short-circuits via `position_to_index(pos)?`.

## Bug 2 — XOR-FEC recovery silently produced 0-byte payloads

**Where:** `src/packet_processor/fec/mod.rs`.

**Symptom:** Whenever XOR-FEC recovered a missing data packet, the resulting `RecoverdPacket` had `payload.truncate(0)` and `WriteableBuffer::write` then dropped it (`to_write.len() != MAX_PAYLOAD_LENGTH` and not the last chunk). On localhost the path almost never triggered (no loss), but the forward-seek fix from Bug 1 made retransmit batches large and bursty, exercising the broken recovery path and surfacing data corruption.

**Cause:**
- The data-packet → FECPacket path (`From<DataPacket> for FECPacket`) wrote `[byte_range_start(4)] [payload]` and left the trailing bytes zero.
- The recovery side (`From<FECData> for RecoverdPacket`) reads `payload_len` from **the last 2 bytes of FECData**.
- The two layouts disagreed → recovered length was always 0.

A separate `From<DataPacket> for FECData` impl existed and *did* write a length, but at offset `4 + payload.len()` instead of end-of-FECData; it was unused dead code.

**Fix:**
- Renamed `From<DataPacket> for FECData` to `From<&DataPacket> for FECData` (so we can construct without consuming).
- Wrote `payload.len() as u16` at `FEC_DATA_SIZE - 2` — the offset the reader actually uses. For full-length payloads this matches the reader exactly. (Partial last chunk is still mis-aligned, but that's a follow-up.)
- Replaced the manual construction in `From<DataPacket> for FECPacket` with `FECData::from(&value)` so send-time and recovery-time layouts stay in lockstep.

Verified with a one-off `debug!` that recoveries went from `payload_len=0` (48/48 events) to `payload_len=1384` after the fix.

## Bug 3 — `holes(3)` left a permanent blind spot at end-of-stream

**Where:** `src/manager/state.rs::ProtocolState::holes` and `src/api/types.rs::WriteableBuffer::find_holes`.

**Symptom:** `holes(3)` only returns chunks below `0.75 · head`. Once `head` reaches `len`, the chunks inside the top 25% are never re-requested. After a forward seek with even a single packet drop in that band, the receiver permanently misses bytes there but still flips `is_done`, sends `Done`, and the client-side assert fails.

**Fix (split across the receiver):**
- `WriteableBuffer::is_done()` is now **strict**: `head >= len` AND every map entry is filled. A new `head_at_end()` returns the old (head>=len) condition for callers that need the looser check.
- `WriteableBuffer::find_holes(until)` extends to scan the *full* map (including the trailing partial-chunk index) when `until >= self.len()`. Below that threshold the previous bound is preserved.
- `received_data_packet` and `recovered_packet`: when `head` first crosses `len`, fire a one-time `holes(4)` sweep + retransmit request — the "end-of-session sweep". Done is declared only when `(complete_allow_partial && head_at_end) || is_done()` (strict).

`complete_allow_partial` semantics also tightened per user request: it still requires `head >= len`, but does **not** require all chunks to be filled. Use case: user wants to finish the stream even though some bytes were skipped on purpose.

The end-of-session sweep is *one-shot*. If the request or response is lost, the receiver hangs. This is the known weakness — see "Open issue" below.

## Bug 4 — `batch_pos == batch_size` was off-by-one (dead code)

**Where:** `src/manager/state.rs::received_data_packet`.

**Symptom:** The original "trigger steady-state retransmit on batch boundary" check was `packet.fec_info.batch_pos == packet.fec_info.batch_size`. Data packets have `batch_pos ∈ [0, batch_size)`, so this never fired for any data packet. With the forward-seek fix making retransmit-driven streams real for the first time, the dead branch became load-bearing.

**Fix:** changed to `batch_pos + 1 == batch_size` (this packet is the last data packet of its batch). Now `check_for_retransmits(holes(3))` actually runs as originally intended.

## Bug 5 — `Vec<ByteRange>` wire format wasted 2 bytes per range, and packet limits used in-memory size

**Where:** `src/manager/packets.rs` (`Vec<ByteRange>` Serialize impl, `RetransmitPacket::data` / `metadata` size assertions).

**Symptom:** While debugging Bug 3 the receiver tried to pack a 230-range retransmit request into one packet, panicking at the assertion in `RetransmitPacket::data` ("172 ByteRanges max").

**Cause:** `ByteRange::sized()` is correctly `4 + 2 = 6` bytes (derived), but `Vec<ByteRange>::{serialize, deserialize, sized}` and the two `RetransmitPacket::data`/`metadata` assertions all hardcoded `size_of::<ByteRange>()` — which is `8` thanks to alignment padding around the `u32`. That made the on-wire format 8 bytes per range with 2 bytes of garbage, and miscomputed the per-packet capacity.

**Fix:**
- New `ByteRange::elem_size()` inline associated fn returning the on-wire size by calling `Serialize::sized` on a constructed instance.
- Made `ByteRange::new` `const fn` (the `length` overflow check now uses a non-interpolated literal so it's const-compatible).
- Replaced all four `size_of::<ByteRange>()` sites with `ByteRange::elem_size()` (`Vec<ByteRange>` serialize/deserialize/sized + two assertions).
- Made `RetransmitPacket::LOCAL_MAX_PAYLOAD_LENGTH` `pub` so the new `state.rs::send_retransmit_requests` can size chunks correctly: `LOCAL_MAX_PAYLOAD_LENGTH / ByteRange::elem_size()` (= 230).

Net effect: tighter packing on the wire (25% denser for retransmit packets) and the assertion no longer trips.

## Bug 6 — Failing tests leaked orphan integration server processes

**Where:** `tests/e2e_test.rs`.

**Symptom:** When a test panicked (e.g. on `expect("client timed out")`), the still-running server `Child` was just dropped. Rust's `std::process::Child` doesn't kill on drop, so the server kept running with `PPID = 1` (reparented to systemd). Worse, that orphan inherited the `cargo test` stdout pipe, so any `cargo test … | tail` invocation hung indefinitely after `cargo test` exited.

**Fix:** added a `KillOnDrop(std::process::Child)` newtype in `tests/e2e_test.rs`, with `Drop` calling `kill()` and `wait()`. Used `derive_more::{Deref, DerefMut}` (already a workspace dependency) so `wait_timeout(&mut server, …)` and friends keep working unchanged. All 16 spawn sites (8 server, 8 client) now go through `KillOnDrop::spawn`.

Verified: failing runs no longer leave any `integration --side server` process behind, and `cargo test … | tail` no longer hangs on EOF.

## Files changed

| File | What |
|---|---|
| `src/api/types.rs` | `seek_head` rewrite; strict `is_done`; new `head_at_end`; `find_holes` full-map at end |
| `src/manager/state.rs` | `received_data_packet` + `recovered_packet` rewritten; one-time end-of-session sweep; `send_retransmit_requests` extracted; `batch_pos + 1 == batch_size`; chunk size via `ByteRange::elem_size()`; partial-allow waits for `head >= len` |
| `src/packet_processor/fec/mod.rs` | `From<&DataPacket> for FECData` writes length at end-of-FECData; `From<DataPacket> for FECPacket` delegates |
| `src/manager/packets.rs` | `ByteRange::new` is `const`; `ByteRange::elem_size()` inline assoc fn; `Vec<ByteRange>` impls + two assertions use it; `RetransmitPacket::LOCAL_MAX_PAYLOAD_LENGTH` made `pub` |
| `tests/e2e_test.rs` | `KillOnDrop(Child)` newtype, all 16 spawn sites routed through it |

No dependencies added.

## Final test state

- 47 unit / packet-processor / macro / transport tests: passing.
- 6 / 8 e2e tests passing.
- `audio_data` (ignored): passing.
- `audio_data_with_playback_control` (ignored), `test_with_seek`: **flaky** under localhost UDP loss. Roughly 1/3 of runs pass; the rest hit the test's 10 s timeout because of remaining placeholder-quality issues in the retransmit path.

## Retransmit-hardening (round 2)

After the bullets above, we layered ack-retry tuning + batch-counter resweep + a few discovered protocol bugs onto the same code paths:

### Ack retry interval (`A`)
- `PACKET_DISCARD_TIME_MS`: `7 s` → **500 ms** (in `src/manager/state.rs`).
- `PendingAckWindow::BUFFERING_TIME`: `2 s` → **100 ms** (formerly underflowed `PRUNE - BUFFERING` when reduced).

`RetransmitPacket` already sets `RequireAck` and is registered in `PendingAckWindow` via `add_ack!`, so a lost retransmit-request is now actually re-sent within ~600 ms instead of after ~9 s (longer than the test's 10 s timeout).

### Batch-counter resweep (`C`)
- New `SWEEP_BATCH_THRESHOLD = 1` (in `src/manager/state.rs`).
- New field `WriteableBuffer::batches_since_sweep: u32` with `note_batch_end` / `reset_sweep_counter` helpers (`src/api/types.rs`).
- `received_data_packet` now also re-fires the end-of-session sweep when `head_at_end && batch_end && !is_done()`, so a lost retransmit-response triggers another request on the next batch boundary instead of a permanent hang. The counter is kept in `received_data_packet` only — `recovered_packet` would otherwise double-count the same data+recovery batch.

### Receiver-side fixes that surfaced while wiring A+C
- `src/manager/state.rs::received_data_packet` — the original `batch_pos == batch_size` check (line 397 in current code) was off-by-one and never fired for data packets; widened to `u16` and changed to `+ 1 == batch_size` so it correctly fires on the last data packet of a batch and doesn't overflow when batches grow past 256 chunks.

### `RetransmitPacket` server-side authentication
**Bug:** `src/packet_processor/inbound.rs` deserialized first and then called `authenticate(&mut packet.headers(), …)`. The sender's tag covers the **full serialized packet**, not just the header bytes — so server-side auth was failing 100% of the time and dropping every retransmit request before it even reached `received_retransmit_request`.
**Fix:** match the `Close` and `Playback` patterns: `authenticate(&mut data, …)` on the full bytes first, then deserialize.

### `Vec<ByteRange>` wire format / capacity (re-using `ByteRange::elem_size()` from earlier)
The earlier `Vec<ByteRange>` change to use `ByteRange::elem_size()` (= 6) for stride and chunk count exposed:
- An off-by-one in `Vec<ByteRange>::deserialize` — the loop pattern `if buf.len() >= stride { flush; clear } push` never flushes the **trailing** chunk, so an `M`-range request was decoded as `M-1` ranges. For 1-range requests, the server received 0 ranges and replied with a 0-chunk batch; this was the source of the "60 batches of 0 packets" symptom in the failing logs. Replaced the loop with `bytes.chunks(stride).map(ByteRange::deserialize).collect()`.

### FEC outbound race
`src/packet_processor/fec/xor.rs::OutboundBatchData::add` returned `true` whenever `current_size >= batch_size`. Once a batch saw any over-fill (e.g. caused by the `current_batch` non-uniqueness fix below), every subsequent caller saw `true` and raced to `outbound.remove_entry(...)`; the slow ones panicked with `"invariant borken: batch does not exist"`. Tightened to `current_size == batch_size` so exactly one caller proceeds.

### `current_batch` atomic
`StreamingTo::current_batch` was `BatchID` with a `**current_batch += 1; *current_batch` increment-then-read pattern, valid only because `lock_write!(connections)` happened to serialize callers. Switched to `AtomicU16` with a new `StreamingTo::next_batch_id(&self) -> BatchID` helper, removing the implicit dependency on the shared lock and ensuring concurrent `retransmit_action` and `send_stream_action` callers can never observe the same batch id.

## Open issue: lossy retransmit, residual flakiness

`test_with_seek` passes ~30% of the time and times out the rest of the time. With all the above fixes, the failure mode is still localhost UDP loss exceeding what the placeholder retransmit machinery can recover from before the test's 10 s timeout — *not* corruption. Per the user, the retransmit layer is being redesigned and shouldn't be hardened further now.

---

# Retransmit redesign (subsequent session)

This section covers the retransmit-policy rewrite that replaced the placeholder machinery referenced above. **Final state: 23/23 lib + 6/6 e2e in parallel, ~3 s.**

## Score-based receiver policy on `WriteableBuffer`

Built on top of an `Area(Range<usize>)` newtype tracking contiguous invalid chunk runs. `WriteableBuffer.invalid_areas: Vec<Area>` mirrors `!map`, maintained incrementally in `occupy()`.

Per-area score = **urgency × confidence_lost**:

- **Urgency** — `1 / (1 + distance_from_head_in_chunks / URGENCY_DECAY_CHUNKS)` for after-head holes; `min(BEFORE_HEAD_URGENCY_CAP, size / BEFORE_HEAD_SIZE_DENOM)` for before-head (seek-leftover) holes. Head-adjacent peaks at 1; before-head capped strictly below 1 so live progress always wins on urgency alone.
- **Confidence-lost** — `valid_past / (valid_past + CONFIDENCE_HALF_SAT_CHUNKS)` where `valid_past = total_chunks - area.end - sum_of_later_invalid`. Proxy for "this hole has been overtaken by enough later arrivals to be confidently real loss vs. still in flight."

Tunables (`SCORE_THRESHOLD = 0.3`, `MAX_REQUESTS_PER_TICK = 2`) on `WriteableBuffer`. `requestable_areas()` is the policy output: filter by threshold, sort descending by score, truncate.

9 tests in `src/api/types.rs::tests` cover urgency monotonicity, confidence shape, before-head cap, and the threshold + per-tick selection cap.

## FEC inbound batch lifecycle (TTL + missing-chunks emission)

Was the user's flagged leak: full-data batches with no parity sat in FEC's inbound HashMap forever (only `recover` removes entries, and `recover` only fires when the batch is recoverable).

- `InboundBatchData` (both XOR and RS) gained `created_at: Timestamp`, `base_byte_pos: Option<BytePosition>`, `is_contiguous: bool`. Captured on first data packet from `byte_range_start - batch_pos × MPL`.
- `Xor::prune(ttl_ms)` / `RS::prune(ttl_ms)`: snapshot `(key, Arc<Mutex<batch>>)` under brief outer lock, check timestamps without holding outer, re-acquire briefly to remove expired entries, compute `missing_positions` from the snapshot. Outer mutex hold time is two short windows instead of "duration of full iteration", so concurrent `received` / `recover` doesn't block.
- Module-level `fec::prune(ttl_ms)`.

## Manager-side mirror (`SessionFecState`)

Per user direction "minimum state bleed": the manager builds its own per-session view of in-flight batches purely from `DataPacket` data, no API into FEC's internal state.

- `SessionFecState` (already a field on `StreamState`) was dormant; activated.
- Refactored to plain `HashMap<BatchID, FecBatchWindow>` (no internal `RwLock` — always accessed under outer connections lock now).
- `FecBatchWindow` extended with `created_at`, `base_byte_pos`, `is_contiguous` (same triple as the FEC-internal `InboundBatchData`).
- `add_data` wired into `received_data_packet`; `evict` called on `recovered_packet`.
- `active_byte_ranges()` does TTL eviction lazily on read (retain stale-out, then yield contiguous-batch byte ranges). Score policy filters its candidates against this list — chunks still actively being received via the primary path are skipped.

## Pending dedup: `StreamingFrom`

To prevent the score policy and the FEC TTL prune from stepping on each other (each can independently want to request the same chunks), `Streaming::From(WriteableBuffer)` was promoted to `Streaming::From(StreamingFrom)` carrying:

```rust
pub struct StreamingFrom {
    pub buffer: WriteableBuffer,
    pub pending: HashMap<usize, Timestamp>,  // FxHashMap via prelude alias
}
```

`reserve_for_request(positions) -> Vec<BytePosition>` is the single dedup gate: drops chunks already filled or already pending, marks accepted ones, opportunistically sweeps stale (older than `PENDING_TTL_MS = PACKET_DISCARD_TIME_MS`) before checking. `clear_pending(pos)` fires from `received_data_packet` and `recovered_packet` whenever data arrives. Lazy sweep avoids the global write-lock contention that an earlier per-tick global sweep caused (broke `test_data_bigger_than_packet`).

## FEC TTL prune task in the manager

Mirrors `PendingAckWindow` shape. `FecPruneTask::prune` runs every 100 ms:
1. `fec::prune(TTL_MS)` evicts stale batches and returns `(session_id, batch_id, missing_positions)`.
2. Group results by `session_id` (saves N-1 connections-write-lock acquisitions per session per tick).
3. Per session: take the connections lock once, route the union of missing positions through `streaming_from.reserve_for_request(...)` for dedup, dispatch the accepted subset.

Added `fec_prune.close()` to the `outbound::init`'s `ApiCommand::Close` handler — without it, `global_handle_monitor.flush().await` waits forever on the prune loop and `Api::drop` hangs forever on `manager_handle.join()`. (That was the original "shutdown-broken" symptom.)

## Auto-ack on `RetransmitPacket` (the seek-test fix)

`received_retransmit_request` (`routines/received.rs:52`) didn't call `received_packet_that_requires_ack`, even though every `RetransmitPacket` has `RequireAck` set. Result: every retransmit-request the receiver sent sat unacked in its `PendingAckWindow` and got retried up to `MAX_RETRIES = 5` times every 500 ms. Stacked with new requests from the score policy + FEC TTL prune, the ack-window backlog grew monotonically and exploded the outbound rate (the "2 → 172 batches/sec" ramp under seek). The server appeared to stop responding because its UDP inbound queue was firehosed.

Fix: mirror the `received_data_packet` ack call. One-liner.

## Unified send path (kills retransmit-action burst dispatch)

The remaining seek-test failure (which still showed up under parallel-test contention) was that **retransmit responses bypassed the `send_stream_action` interval pacing entirely**. `retransmit_action` was a separate dispatch firing on `StreamEvent::Retransmit` that read all requested chunks at once and sent them in one burst — 1,500 packets in <100 ms for a typical seek-skipped region. UDP socket buffers (~140 packets at default Linux settings) couldn't keep up; the receiver dropped most of them.

Refactor:
- Dropped `retransmit_action`. Dropped `paused_retransmits`. Dropped `seek_holes`.
- Single per-session `extras: Vec<ByteRange>` queue in the run-streaming loop. Holds chunk-sized `ByteRange`s (split via new `split_range_into_chunks`).
- `StreamEvent::Retransmit(ranges)` extends `extras` with the chunk-split versions. No ad-hoc dispatch.
- `send_stream_action`: when `get_chunks` is empty AND `extras` is non-empty, drains at most `n = PACKET_COUNT_PER_BATCH` from `extras` per tick. Head sends always win — extras are only touched once fresh data is exhausted.
- `StreamState::retransmit` simplified, off-by-one bug killed (the `for _ in 0..=(length / MPL)` was sending one extra chunk per range).
- `BytePosition::to_range` removed (silently truncated to `u16` length for seeks > 65,535 bytes — a separate bug surfaced during diagnosis). Replaced with chunk-aligned `push_seek_hole_chunks` at the call site.

Net: retransmits inherit the same rate ceiling as fresh data. No more bursts. The ramp is gone.

## Locking review

After the prune task landed, granularity-tuned in two places:

- `FecPruneTask::prune`: group expired batches by `session_id` before processing, so the connections write lock is acquired once per session instead of once per batch.
- `Xor::prune` / `RS::prune`: snapshot `(key, Arc<Mutex<batch>>)` under brief outer lock, drop, then iterate the snapshot for timestamp checks. Outer `inbound` mutex hold time goes from "full iteration" to "two short windows."

## `SEND_INTERVAL` sweep

After the unified send path landed, swept the interval to characterize the design space. 3 runs each at stable values, 8 runs at the unstable ones.

| `SEND_INTERVAL` | Pass rate | Wall time | Notes |
|---|---|---|---|
| 5 ms  | 3/3        | 1.30 s | Fastest. Aggressive on syscalls. |
| 10 ms | 7/8 (1 flake on `test_data_bigger_than_packet`) | 1.85 s | Edge of stability. |
| 15 ms | 3/3        | 2.10 s | **Sweet spot — fast and stable.** |
| 25 ms | 3/3        | 3.0–3.4 s | Current default. Comfortable margin. |
| 50 ms | 3/3        | 5.56 s | Stable, throughput-bound. |
| 100 ms | 2/3 (1 flake on a slow handshake) | 10 s | Test budget too tight. |

Below ~15 ms the protocol pushes packets faster than handshake-ack timing can give other tests headroom — flakes appear in non-seek tests. Above ~50 ms, throughput-bound; 100 ms hits the test framework's 10 s wall-clock with no margin. 25 ms current default is fine; 15 ms would be a free ~30% speedup at a small flakiness cost.

The seek test specifically: passes consistently across **all** stable intervals because the retransmit response is bounded to `n` chunks per tick now, regardless of how big the seek-skipped region is.

## Files changed (this round)

| File | What |
|---|---|
| `src/api/types.rs` | `Area` newtype with associated-const tunables and `urgency`/`confidence_lost` methods; `WriteableBuffer.invalid_areas` + maintenance in `occupy`; `score_areas` / `requestable_areas`; tests; removed dead `note_batch_end`/`reset_sweep_counter`. |
| `src/manager/state.rs` | `StreamingFrom` struct (replaces `Streaming::From(WriteableBuffer)`) with pending-marker dedup; `score_policy_pick` with FEC-active filter; `FecPruneTask`; unified `extras` queue in run-streaming loop; per-tick budget on extras drain; `push_seek_hole_chunks` + `split_range_into_chunks`; `dispatch_retransmit_request`; locking-granularity tuning. Removed: `retransmit_action`, `holes`, `send_retransmit_requests`, `SWEEP_BATCH_THRESHOLD`, sweep-counter machinery. |
| `src/manager/routines/received.rs` | `received_retransmit_request` takes `outbound_sender` and auto-acks on `RequireAck`; diagnostic log. |
| `src/manager/routines/endpoints.rs` | `find_holes` API endpoint switches to raw `WriteableBuffer::find_holes` (no policy coupling). |
| `src/manager/inbound.rs` | Pass `outbound_sender` through to `received_retransmit_request`. |
| `src/manager/outbound.rs` | `Close` handler also closes `fec_prune` task — fixes `Api::drop` hang. |
| `src/manager/mod.rs` | Spawn the FEC prune task at startup. |
| `src/manager/packets.rs` | Removed `BytePosition::to_range` (silently `u16`-truncating); added `BytePosition`↔`usize` `PartialEq`/`PartialOrd`. |
| `src/packet_processor/fec/{mod.rs, xor.rs, reed_solomon.rs}` | `InboundBatchData` extended with timestamp + base + contiguity; `prune(ttl_ms)`; `missing_positions`; module-level `fec::prune`. |

No dependencies added.

## Final test state

- `cargo test --lib`: **23/23**.
- `cargo test --test e2e_test` (parallel): **6/6** in ~3 s.
- `cargo test --test e2e_test -- --test-threads=1`: 6/6.
- 2 e2e tests (`audio_data*`) remain `#[ignore]`'d as before.
