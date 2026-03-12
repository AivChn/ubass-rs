pub trait PacketSerialize {
    fn serialize(&self, buf: &mut [u8]) -> bool;
    fn sized(&self) -> usize;
}

macro_rules! impl_packet_serialization_ints {
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

            impl PacketDeserialize for $t {
                #[inline]
                fn deserialize(bytes: &[u8]) -> Option<Self> {
                    Some(
                        <Self>::from_be_bytes(
                            <[u8; size_of::<Self>()]>::try_from(bytes.get(..size_of::<Self>())?).ok()?,
                        )
                    )
                }
            }
        )*
    };
}

impl_packet_serialization_ints!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

impl PacketSerialize for Vec<u8> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.len() {
            false
        } else {
            buf.copy_from_slice(self);
            true
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        self.len()
    }
}

impl<const N: usize> PacketSerialize for [u8; N] {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N {
            false
        } else {
            buf.copy_from_slice(self);
            true
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        N
    }
}

impl<const N: usize> PacketSerialize for Box<[u8; N]> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N {
            false
        } else {
            buf.copy_from_slice(&**self);
            true
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        N
    }
}

pub trait PacketDeserialize: Sized {
    fn deserialize(bytes: &[u8]) -> Option<Self>;
}

impl PacketDeserialize for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Vec::from(bytes))
    }
}

impl<const N: usize> PacketDeserialize for Box<[u8; N]> {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Box::new(bytes.get(..N)?.try_into().ok()?))
    }
}
