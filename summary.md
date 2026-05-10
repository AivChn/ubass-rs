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
