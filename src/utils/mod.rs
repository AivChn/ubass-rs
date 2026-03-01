use std::time::Duration;

use tokio::{sync::Mutex, task::JoinHandle};

#[macro_export]
macro_rules! dispatch {
    ($call: expr, $monitor: expr) => {
        let handle = tokio::spawn($call);
        $monitor.add(handle).await;
    };
}

pub struct HandleMonitor(pub Mutex<Vec<JoinHandle<()>>>);

impl HandleMonitor {
    pub fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    pub async fn size(&self) -> usize {
        let handles = self.0.lock().await;
        handles.len()
    }

    pub async fn init(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            self.prune().await;
        }
    }

    pub async fn add(&self, handle: JoinHandle<()>) {
        let mut handles = self.0.lock().await;
        handles.push(handle);
    }

    pub async fn prune(&self) {
        let mut handles = self.0.lock().await;
        handles.retain(|h| !h.is_finished());
    }

    pub async fn flush(&self) {
        let mut handles = self.0.lock().await;
        for handle in handles.drain(..) {
            handle.await;
        }
    }
}
