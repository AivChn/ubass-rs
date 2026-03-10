use std::sync::Arc;

use crate::packetizer::types::SessionId;

pub struct TmpAddressSessionMapper {
    factor: u64,
}

pub struct AddressSessionMonitor {
    table: Arc<TmpAddressSessionMapper>,
}

impl AddressSessionMonitor {
    pub async fn get_session_id(&self, addr: (u8, u8, u8, u8, u16)) -> SessionId {
        let full: u64 = (addr.0 as u64) << 40
            | (addr.1 as u64) << 32
            | (addr.2 as u64) << 24
            | (addr.3 as u64) << 16
            | addr.4 as u64;
        SessionId(full * self.table.factor)
    }

    pub async fn get_addr(&self, session_id: SessionId) -> String {
        let unfactored = session_id.0 / self.table.factor;
        let a = ((unfactored >> 40) & 0xFF) as u8;
        let b = ((unfactored >> 32) & 0xFF) as u8;
        let c = ((unfactored >> 24) & 0xFF) as u8;
        let d = (unfactored >> 16) & 0xFF;
        let p = (unfactored & 0xFFFF) as u16;

        format!("{}.{}.{}.{}:{}", a, b, c, d, p)
    }
}

impl Default for AddressSessionMonitor {
    fn default() -> Self {
        Self {
            table: Arc::new(TmpAddressSessionMapper { factor: 333333333 }),
        }
    }
}
