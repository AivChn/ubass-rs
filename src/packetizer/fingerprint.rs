pub trait HeaderSerialize {
    fn headers(&self) -> Vec<u8>;
}

pub trait Fingerprint: HeaderSerialize {
    fn fingerprint(&self) -> [u8; 16];
}

impl<T: HeaderSerialize> Fingerprint for T {
    fn fingerprint(&self) -> [u8; 16] {
        xxhash_rust::xxh3::xxh3_128(&self.headers()).to_be_bytes()
    }
}
