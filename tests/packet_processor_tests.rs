#![cfg(test)]

#[cfg(test)]
mod encryption {
    use std::sync::LazyLock;

    use aes_gcm_siv::{Aes256GcmSiv, KeyInit, aead::OsRng};
    use ubass::{
        manager::{
            EncryptionMonitor,
            state::{EncryptionTable, EncryptionWindow},
        },
        packet_processor::{
            encryption::{self, Encryptable},
            fingerprint::{Headers, Payload},
        },
        prelude::packets::SessionId,
    };

    #[derive(Debug, Clone, PartialEq)]
    struct EncryptionTestWrapper(Vec<u8>);
    impl Encryptable for EncryptionTestWrapper {}

    impl Payload for EncryptionTestWrapper {
        fn payload(&mut self) -> &mut Vec<u8> {
            &mut self.0
        }
    }

    impl Headers for EncryptionTestWrapper {
        fn headers(&self) -> Vec<u8> {
            Vec::new()
        }
    }

    static ENCRYPTION: LazyLock<EncryptionTable> = LazyLock::new(EncryptionTable::default);

    fn get_window() -> EncryptionWindow {
        EncryptionWindow::new(Aes256GcmSiv::new(&Aes256GcmSiv::generate_key(&mut OsRng)))
    }

    #[tokio::test]
    async fn encrypt_decrypt() {
        let window = get_window();
        ENCRYPTION.write().await.insert(SessionId::new(1), window);

        let encryption_monitor = EncryptionMonitor::new(&ENCRYPTION);
        let mut payload = EncryptionTestWrapper(Vec::from(b"Hello World!"));
        let correct = payload.clone();
        encryption::encrypt(&mut payload, SessionId::new(1), encryption_monitor).await;
        assert!(
            encryption::decrypt(&mut payload, SessionId::new(1), encryption_monitor)
                .await
                .is_ok()
        );
        assert_eq!(correct, payload);
    }

    #[tokio::test]
    async fn encrypt_modify_decrypt() {
        let window = get_window();
        ENCRYPTION.write().await.insert(SessionId::new(2), window);

        let encryption_monitor = EncryptionMonitor::new(&ENCRYPTION);
        let mut payload = EncryptionTestWrapper(Vec::from(b"Hello World!"));

        encryption::encrypt(&mut payload, SessionId::new(2), encryption_monitor).await;
        payload.0[0] = !payload.0[0];
        assert!(
            encryption::decrypt(&mut payload, SessionId::new(2), encryption_monitor)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn tag_authenticate() {
        let window = get_window();
        ENCRYPTION.write().await.insert(SessionId::new(3), window);

        let encryption_monitor = EncryptionMonitor::new(&ENCRYPTION);
        let mut payload = Vec::from(b"Hello World!");

        encryption::tag(&mut payload, SessionId::new(3), encryption_monitor).await;
        let authenticated =
            encryption::authenticate(&mut payload, SessionId::new(3), encryption_monitor).await;
        assert!(authenticated);
    }

    #[tokio::test]
    async fn tag_modify_authenticate() {
        let window = get_window();
        ENCRYPTION.write().await.insert(SessionId::new(4), window);

        let encryption_monitor = EncryptionMonitor::new(&ENCRYPTION);
        let mut payload = Vec::from(b"Hello World!");

        encryption::tag(&mut payload, SessionId::new(4), encryption_monitor).await;
        payload[0] = !payload[0];
        let authenticated =
            encryption::authenticate(&mut payload, SessionId::new(4), encryption_monitor).await;
        assert!(!authenticated);
    }
}
