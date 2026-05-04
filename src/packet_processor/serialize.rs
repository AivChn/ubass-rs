use crate::prelude::*;
use std::net::{Ipv4Addr, SocketAddrV4};

pub trait Serialize: Sized {
    /// # Errors
    /// This errors if buffer not big enough
    #[allow(clippy::result_unit_err)]
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult;
    /// # Errors
    /// this errors if deserialization fails
    #[allow(clippy::result_unit_err)]
    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()>;
    fn sized(&self) -> usize;
}

macro_rules! impl_packet_serialization_ints {
    ($($t:ty),*) => {
        $(
            impl Serialize for $t {
                #[inline]
                fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
                    if buf.len() < self.sized() {
                        Err(())
                    } else {
                        buf[..self.sized()].copy_from_slice(&self.to_be_bytes());
                        Ok(())
                    }
                }

                #[inline]
                fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
                    Ok(Self::from_be_bytes(
                        <[u8; size_of::<Self>()]>::try_from(
                            bytes
                                .get(..size_of::<Self>())
                                .ok_or(())?
                        )
                        .map_err(|_| ())?,
                    ))
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
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.sized() {
            Err(())
        } else {
            self.ip().octets().serialize(buf)?;
            self.port().serialize(&mut buf[4..])?;
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        if bytes.len() < 6 {
            Err(())
        } else {
            let octets = <[u8; 4]>::deserialize(bytes)?;
            let port = u16::deserialize(&bytes[4..])?;

            Ok(SocketAddrV4::new(Ipv4Addr::from_octets(octets), port))
        }
    }

    #[inline]
    fn sized(&self) -> usize {
        6
    }
}

impl Serialize for Vec<u8> {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.len() {
            Err(())
        } else {
            buf[..self.len()].copy_from_slice(self);
            Ok(())
        }
    }

    #[inline]
    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        Ok(Vec::from(bytes))
    }

    #[inline]
    fn sized(&self) -> usize {
        self.len()
    }
}

impl<const N: usize> Serialize for [u8; N] {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < N {
            Err(())
        } else {
            buf[..N].copy_from_slice(self);
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        bytes.get(..N).ok_or(())?.try_into().map_err(|_| ())
    }

    #[inline]
    fn sized(&self) -> usize {
        N
    }
}

impl<const N: usize> Serialize for Box<[u8; N]> {
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < N {
            Err(())
        } else {
            buf[..N].copy_from_slice(&**self);
            Ok(())
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        Ok(Box::new(
            bytes.get(..N).ok_or(())?.try_into().map_err(|_| ())?,
        ))
    }

    #[inline]
    fn sized(&self) -> usize {
        N
    }
}

impl<T1, T2> Serialize for (T1, T2)
where
    T1: Serialize,
    T2: Serialize,
{
    fn serialize(&self, buf: &mut [u8]) -> EmptyResult {
        if buf.len() < self.sized() {
            Err(())
        } else {
            self.0.serialize(buf)?;
            self.1.serialize(&mut buf[self.0.sized()..])
        }
    }

    fn deserialize(bytes: &[u8]) -> core::result::Result<Self, ()> {
        let f1 = T1::deserialize(bytes)?;
        let f2 = T2::deserialize(&bytes[f1.sized()..])?;
        Ok((f1, f2))
    }

    fn sized(&self) -> usize {
        self.0.sized() + self.1.sized()
    }
}
