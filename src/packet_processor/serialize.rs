use std::net::{Ipv4Addr, SocketAddrV4};

pub trait Serialize: Sized {
    fn serialize(&self, buf: &mut [u8]) -> bool;
    fn deserialize(bytes: &[u8]) -> Option<Self>;
    fn sized(&self) -> usize;
}

macro_rules! impl_packet_serialization_ints {
    ($($t:ty),*) => {
        $(
            impl Serialize for $t {
                #[inline]
                fn serialize(&self, buf: &mut [u8]) -> bool {
                    if buf.len() < self.sized() {
                        false
                    } else {
                        buf[..self.sized()].copy_from_slice(&self.to_be_bytes());
                        true
                    }
                }

                #[inline]
                fn deserialize(bytes: &[u8]) -> Option<Self> {
                    Some(
                        <Self>::from_be_bytes(
                            <[u8; size_of::<Self>()]>::try_from(bytes.get(..size_of::<Self>())?).ok()?,
                        )
                    )
                }

                #[inline]
                fn sized(&self) -> usize {
                    size_of::<$t>()
                }

            }
        )*
    };
}

impl_packet_serialization_ints!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

impl Serialize for SocketAddrV4 {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.sized() {
            false
        } else {
            self.ip().octets().serialize(buf);
            self.port().serialize(&mut buf[4..]);

            true
        }
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 6 {
            None
        } else {
            let octets = <[u8; 4]>::deserialize(bytes).expect("size guaranteed");
            let port = u16::deserialize(&bytes[4..]).expect("size guaranteed");

            Some(SocketAddrV4::new(Ipv4Addr::from_octets(octets), port))
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        6
    }
}

impl Serialize for Vec<u8> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.len() {
            false
        } else {
            buf[..self.len()].copy_from_slice(self);
            true
        }
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Vec::from(bytes))
    }

    #[inline]
    fn sized(&self) -> usize {
        self.len()
    }
}

impl<T: Serialize + Default + PartialEq> Serialize for Option<T> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < self.sized() {
            false
        } else {
            match self {
                None => T::default().serialize(buf),
                Some(value) => value.serialize(buf),
            }
        }
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        let value = T::deserialize(bytes)?;

        if value == T::default() {
            Some(None)
        } else {
            Some(Some(value))
        }
    }

    fn sized(&self) -> usize {
        size_of::<T>()
    }
}

impl<const N: usize> Serialize for [u8; N] {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N {
            false
        } else {
            buf[..N].copy_from_slice(self);
            true
        }
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bytes.get(..N)?.try_into().ok()
    }

    #[inline]
    fn sized(&self) -> usize {
        N
    }
}

impl<const N: usize> Serialize for Box<[u8; N]> {
    fn serialize(&self, buf: &mut [u8]) -> bool {
        if buf.len() < N {
            false
        } else {
            buf[..N].copy_from_slice(&**self);
            true
        }
    }

    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(Box::new(bytes.get(..N)?.try_into().ok()?))
    }

    #[inline]
    fn sized(&self) -> usize {
        N
    }
}
