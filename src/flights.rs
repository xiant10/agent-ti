//! One lookup at a time per key: concurrent callers wait for the one doing it, then read what it
//! cached.
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

/// Lookups in flight, by key.
pub struct Flights<K> {
    running: Arc<Mutex<HashMap<K, watch::Receiver<()>>>>,
}
/// A clone shares the lookups in flight.
impl<K> Clone for Flights<K> {
    fn clone(&self) -> Self {
        Flights {
            running: self.running.clone(),
        }
    }
}
impl<K> Default for Flights<K> {
    fn default() -> Self {
        Flights {
            running: Arc::default(),
        }
    }
}
/// Held by the caller doing the lookup. Dropping it, however that caller ends, even cancelled,
/// lets the waiting callers go.
pub struct Flight<K: Eq + Hash> {
    running: Arc<Mutex<HashMap<K, watch::Receiver<()>>>>,
    key: Option<K>,
    _done: watch::Sender<()>,
}
impl<K: Eq + Hash> Drop for Flight<K> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.running.lock().unwrap().remove(&key);
        }
    }
}
impl<K: Eq + Hash + Clone> Flights<K> {
    /// `Some` when this caller should do the lookup. `None` once another caller doing it has
    /// finished, or given up: read its result, and look it up yourself if there's none.
    pub async fn start(&self, key: &K) -> Option<Flight<K>> {
        let mut waiting = {
            let mut running = self.running.lock().unwrap();
            match running.get(key) {
                Some(done) => done.clone(),
                None => {
                    let (tx, rx) = watch::channel(());
                    running.insert(key.clone(), rx);
                    return Some(Flight {
                        running: self.running.clone(),
                        key: Some(key.clone()),
                        _done: tx,
                    });
                }
            }
        };
        // Nothing is ever sent: this ends when the sender is dropped, even if that happened
        // before this call.
        while waiting.changed().await.is_ok() {}
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn waiters_go_when_the_lookup_ends_or_is_cancelled() {
        let flights = Arc::new(Flights::<&str>::default());
        let flight = flights.start(&"a").await.unwrap();
        let waiter = tokio::spawn({
            let flights = flights.clone();
            async move { flights.start(&"a").await.is_none() }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished());
        drop(flight);
        assert!(waiter.await.unwrap());

        // A leader whose task is cancelled mid-lookup still lets the next caller lead.
        let leader = tokio::spawn({
            let flights = flights.clone();
            async move {
                let _flight = flights.start(&"a").await.unwrap();
                std::future::pending::<()>().await;
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        leader.abort();
        let _ = leader.await;
        assert!(flights.start(&"a").await.is_some());
    }
}
