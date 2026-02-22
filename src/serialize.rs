pub trait PacketSerialize {
    fn serialize(&self, buf: &mut [u8]) -> bool;
    fn sized(&self) -> usize;
}

macro_rules! impl_packet_serialize_int {
    ($($t:ty),*) => {
        $(
            impl PacketSerialize for $t {
                #[inline]
                fn serialize(&self, buf: &mut [u8]) -> bool {
                    let size = std::mem::size_of::<$t>();
                    if buf.len() < size {
                        false
                    } else {
                        buf[..size].copy_from_slice(&self.to_be_bytes());
                        true
                    }
                }

                #[inline]
                fn sized(&self) -> usize {
                    size_of::<$t>()
                }
            }
        )*
    };
}

impl_packet_serialize_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

impl PacketSerialize for Vec<u8> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.len() {
            false
        } else {
            buf.copy_from_slice(&self);
            true
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        self.len()
    }
}

impl<T: PacketSerialize + Copy, const N: usize> PacketSerialize for [T; N] {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N * self[0].sized() {
            false
        } else {
            self.iter()
                .enumerate()
                .map(|(i, t)| t.serialize(&mut buf[i..]))
                .all(|e| e)
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        N * std::mem::size_of::<T>()
    }
}

impl<T: PacketSerialize + Copy, const N: usize> PacketSerialize for Box<[T; N]> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N * self[0].sized() {
            false
        } else {
            self.iter()
                .enumerate()
                .map(|(i, t)| t.serialize(&mut buf[(i * self[0].sized())..]))
                .all(|e| e)
        }
    }

    fn sized(&self) -> usize {
        N * std::mem::size_of::<T>()
    }
}

pub trait PacketDeserialize: Sized {
    fn deserialize(bytes: &[u8]) -> Option<Self>;
}

macro_rules! impl_packet_deserialize_int {
    ($($t:ty),*) => {
        $(
            impl PacketDeserialize for $t {
                fn deserialize(bytes: &[u8]) -> Option<Self> {
                    Some(<$t>::from_be_bytes(bytes.get(..size_of::<$t>())?.try_into().ok()?))
                }
            }
        )*
    };
}

impl_packet_deserialize_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

impl PacketDeserialize for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Vec::from(bytes))
    }
}
