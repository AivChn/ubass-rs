use serde::{Deserialize, Serialize};

pub trait PacketSerialize {
    fn serialize(self) -> Vec<u8>;
    fn sized(&self) -> usize;
}

macro_rules! impl_packet_serialize_int {
    ($($t:ty),*) => {
        $(
            impl PacketSerialize for $t {
                fn serialize(self) -> Vec<u8> {
                    self.to_be_bytes().to_vec()
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
    fn serialize(self) -> Vec<u8> {
        self
    }

    #[inline]
    fn sized(&self) -> usize {
        self.len()
    }
}

impl<T: PacketSerialize + Copy, const N: usize> PacketSerialize for [T; N] {
    fn serialize(self) -> Vec<u8> {
        self.iter().flat_map(|e| e.serialize()).collect()
    }

    #[inline]
    fn sized(&self) -> usize {
        N * std::mem::size_of::<T>()
    }
}

impl<T: PacketSerialize + Copy, const N: usize> PacketSerialize for Box<[T; N]> {
    fn serialize(self) -> Vec<u8> {
        self.iter().flat_map(|e| e.serialize()).collect()
    }

    fn sized(&self) -> usize {
        N * std::mem::size_of::<T>()
    }
}

trait PacketDeserialize: Sized {
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
