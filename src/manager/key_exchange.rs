use aes_gcm_siv::aead::OsRng;
use x25519_dalek::{EphemeralSecret, PublicKey};

pub fn create() -> (EphemeralSecret, PublicKey) {
    let secret = EphemeralSecret::random_from_rng(OsRng);
    let pk = PublicKey::from(&secret);
    (secret, pk)
}

pub fn get_shared_secret(secret: EphemeralSecret, other_pk: impl Into<PublicKey>) -> [u8; 32] {
    let shared_secret = secret.diffie_hellman(&other_pk.into());
    shared_secret.to_bytes()
}
