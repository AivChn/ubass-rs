//! Wire format and PRG shared between the data-collection receiver and
//! sender wrappers. Lives in its own module so both wrappers can depend on
//! the same byte-exact functions — if these drifted, the receiver's
//! verification would fail against legitimately-generated server streams.

/// Wire layout of the track-id field carried in `TrackRequestPacket`:
/// `[seed_be: u64 | length_be: u32]`. Sent by the receiver, decoded by the
/// sender.
pub(crate) const TRACK_ID_LEN: usize = 12;

/// Encode `(seed, length)` into the wire-format track id. Big-endian so
/// the format is stable across architectures.
pub(crate) fn encode_track_id(seed: u64, length: u32) -> [u8; TRACK_ID_LEN] {
    let mut out = [0u8; TRACK_ID_LEN];
    out[..8].copy_from_slice(&seed.to_be_bytes());
    out[8..].copy_from_slice(&length.to_be_bytes());
    out
}

/// Decode `(seed, length)` from the wire-format track id. Returns `None`
/// if the slice isn't exactly [`TRACK_ID_LEN`] bytes — the sender must
/// reject such requests.
pub(crate) fn decode_track_id(bytes: &[u8]) -> Option<(u64, u32)> {
    if bytes.len() != TRACK_ID_LEN {
        return None;
    }
    let seed = u64::from_be_bytes(bytes[..8].try_into().ok()?);
    let length = u32::from_be_bytes(bytes[8..].try_into().ok()?);
    Some((seed, length))
}

/// Generate the canonical PRG stream for `seed` into a freshly-allocated
/// buffer of length `length`. The sender hands this to the protocol as the
/// `ReadableBuffer`; the receiver compares its received bytes against the
/// same function via [`verify_prg`].
///
/// Truncation to `u8` is the intended operation — see [`verify_prg`] for
/// the rationale on the byte-mixing rule.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn generate_prg_buffer(seed: u64, length: usize) -> Box<[u8]> {
    let mut state = seed;
    let mut buf = vec![0u8; length];
    for byte in &mut buf {
        let next = splitmix64_step(&mut state);
        *byte = (next as u8) ^ ((next >> 8) as u8);
    }
    buf.into_boxed_slice()
}

/// Verify the received buffer matches the PRG output for `seed`.
///
/// The PRG yields a `u64` per step; we mix the low two bytes into one
/// output byte so a flipped bit anywhere in the low 16 bits of the PRG
/// state shows up. Must remain bit-exact with [`generate_prg_buffer`].
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn verify_prg(seed: u64, received: &[u8]) -> bool {
    let mut state = seed;
    for byte in received {
        let next = splitmix64_step(&mut state);
        if *byte != ((next as u8) ^ ((next >> 8) as u8)) {
            return false;
        }
    }
    true
}

/// Bit-exact `SplitMix64`. Bedrock for the PRG — both wrappers must call
/// this exact function so the bytes line up across the wire.
#[inline]
pub(crate) fn splitmix64_step(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // looser test conventions; lib denies this globally
mod tests {
    use super::*;

    #[test]
    fn track_id_round_trip() {
        let bytes = encode_track_id(0xDEAD_BEEF_CAFE_F00D, 1_234_567);
        let (seed, length) = decode_track_id(&bytes).unwrap();
        assert_eq!(seed, 0xDEAD_BEEF_CAFE_F00D);
        assert_eq!(length, 1_234_567);
    }

    #[test]
    fn decode_rejects_wrong_length() {
        assert!(decode_track_id(&[]).is_none());
        assert!(decode_track_id(&[0u8; TRACK_ID_LEN - 1]).is_none());
        assert!(decode_track_id(&[0u8; TRACK_ID_LEN + 1]).is_none());
    }

    #[test]
    fn generate_then_verify_round_trips() {
        let seed = 0xABCD_1234_5678_9EF0;
        let buf = generate_prg_buffer(seed, 4096);
        assert!(verify_prg(seed, &buf));
        let mut tampered = buf.into_vec();
        tampered[2048] ^= 0xff;
        assert!(!verify_prg(seed, &tampered));
    }

    #[test]
    fn different_seeds_produce_different_buffers() {
        let a = generate_prg_buffer(1, 256);
        let b = generate_prg_buffer(2, 256);
        assert_ne!(*a, *b);
    }
}
