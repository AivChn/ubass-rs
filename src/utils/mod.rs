use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use tokio::{sync::Mutex, task::JoinHandle};

#[macro_export]
macro_rules! dispatch {
    ($call: expr, $monitor: expr) => {
        let handle = tokio::spawn($call);
        $monitor.add(handle).await;
    };
}

pub struct W<T>(pub T);

pub struct HandleMonitor {
    handles: Mutex<Vec<JoinHandle<()>>>,
    destroyed: AtomicBool,
}

impl HandleMonitor {
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(Vec::with_capacity(32)),
            destroyed: AtomicBool::new(false),
        }
    }

    pub async fn size(&self) -> usize {
        let mut handles = self.handles.lock().await;
        handles.retain(|h| !h.is_finished());
        handles.len()
    }

    pub async fn init(self: Arc<Self>) {
        while !self.destroyed.load(std::sync::atomic::Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(250)).await;
            self.prune().await;
        }
    }

    pub async fn add(&self, handle: JoinHandle<()>) {
        let mut handles = self.handles.lock().await;
        handles.push(handle);
    }

    pub async fn prune(&self) {
        let mut handles = self.handles.lock().await;
        handles.retain(|h| !h.is_finished());
    }

    pub async fn flush(&self) {
        self.destroyed
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            handle.await;
        }
    }
}
