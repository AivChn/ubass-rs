pub trait Headers {
    fn headers(&self) -> Vec<u8>;
}

pub trait Fingerprint: Headers {
    fn fingerprint(&self) -> [u8; 16];
}

impl<T: Headers> Fingerprint for T {
    fn fingerprint(&self) -> [u8; 16] {
        xxhash_rust::xxh3::xxh3_128(&self.headers()).to_be_bytes()
    }
}
