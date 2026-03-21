use super::types::*;
pub trait Fingerprint {
    fn fingerprint(&self) -> [u8; 32];
}
