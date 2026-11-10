use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};

// Leases include both owners and waiters. A cancelled waiter cannot unregister
// another request's mutex, and newcomers cannot bypass requests already queued.
pub(crate) struct KeyedFlights<T> {
    entries: Arc<StdMutex<HashMap<String, Arc<T>>>>,
}

impl<T> Clone for KeyedFlights<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Default for KeyedFlights<T> {
    fn default() -> Self {
        Self {
            entries: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

impl<T: Default> KeyedFlights<T> {
    pub(crate) fn lease(&self, key: String) -> FlightLease<T> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = entries.entry(key.clone()).or_default().clone();
        FlightLease {
            registry: self.clone(),
            key,
            entry,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }

    #[cfg(test)]
    pub(crate) fn has_waiters(&self, key: &str) -> bool {
        self.entries
            .lock()
            .unwrap()
            .get(key)
            .is_some_and(|entry| Arc::strong_count(entry) >= 3)
    }

    #[cfg(test)]
    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.entries.lock().unwrap().contains_key(key)
    }
}

// No entry Arc escapes a lease; guards borrow it, so they drop before the lease.
pub(crate) struct FlightLease<T> {
    registry: KeyedFlights<T>,
    key: String,
    pub(crate) entry: Arc<T>,
}

impl<T> Drop for FlightLease<T> {
    fn drop(&mut self) {
        let mut entries = self
            .registry
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if Arc::strong_count(&self.entry) == 2 {
            entries.remove(&self.key);
        }
    }
}

#[derive(Debug)]
pub(crate) struct ResourceLimit;

impl std::fmt::Display for ResourceLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Concurrent work limit reached")
    }
}

impl std::error::Error for ResourceLimit {}
