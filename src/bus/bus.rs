use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{broadcast, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    // add here events to handle
    Shutdown,
}

#[async_trait]
pub trait BusHandler: Send + Sync {
    async fn handle(&mut self, event: Event);
}

pub struct Bus {
    sender: broadcast::Sender<Event>,
}

impl Bus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(32);
        Self { sender: tx }
    }

    pub fn register(&mut self, handler: Arc<Mutex<dyn BusHandler>>) {
        let mut rx = self.sender.subscribe();
        tokio::spawn(async move {
            let handler = handler.clone();

            while let Ok(event) = rx.recv().await {
                let mut handler = handler.lock().await;
                handler.handle(event.clone()).await;

                if event == Event::Shutdown {
                    return;
                }
            }
        });
    }

    pub fn send(&mut self, event: Event) {
        self.sender.send(event).unwrap();
    }
}
