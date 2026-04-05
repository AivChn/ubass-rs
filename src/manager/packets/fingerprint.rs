pub trait Headers {
    fn headers(&self) -> Vec<u8>;
}

pub trait Payload: Headers {
    fn payload(&mut self) -> &mut Vec<u8>;
}

pub trait Fingerprint {
    fn fingerprint(&self) -> [u8; 16];
}

impl Fingerprint for Vec<u8> {
    fn fingerprint(&self) -> [u8; 16] {
        xxhash_rust::xxh3::xxh3_128(self).to_be_bytes()
    }
}

impl<T: Headers> Fingerprint for T {
    fn fingerprint(&self) -> [u8; 16] {
        xxhash_rust::xxh3::xxh3_128(&self.headers()).to_be_bytes()
    }
}
