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
                    Some(unsafe {
                        std::mem::transmute(
                            <[u8; size_of::<Self>()]>::try_from(bytes.get(..size_of::<Self>())?).ok()?,
                        )
                    })
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

impl PacketDeserialize for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Vec::from(bytes))
    }
}

impl<T, const N: usize> PacketDeserialize for [T; N]
where
    T: PacketDeserialize + Default + Copy + Sized,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < std::mem::size_of::<Self>() {
            None
        } else {
            let size = std::mem::size_of::<T>();
            let mut res = Vec::with_capacity(N);
            let mut acc = Vec::with_capacity(size);
            let mut i = 0;

            while i < bytes.len() {
                while acc.len() < size {
                    acc.push(bytes[i]);
                    i += 1;
                }

                res.push(T::deserialize(&acc)?);
                acc.clear();
            }

            Some(res.try_into().ok()?)
        }
    }
}

impl<T, const N: usize> PacketDeserialize for Box<[T; N]>
where
    T: PacketDeserialize + Default + Copy + Sized,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Box::new(<[T; N]>::deserialize(bytes)?))
    }
}
