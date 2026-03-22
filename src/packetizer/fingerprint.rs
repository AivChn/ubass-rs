pub trait Fingerprint {
    fn fingerprint(&self) -> [u8; 16];
}
