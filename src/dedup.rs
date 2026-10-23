//! Shared, bounded FIFO deduplication, matching Go natslink v1.8.3.

use rapidhash::v3::{rapidhash_v3_seeded, RapidSecrets};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DEFAULT_ENTRIES: usize = 4096;

#[derive(Clone, Copy, Default)]
struct Entry {
    key: u64,
    at: u64,
}

struct State {
    ring: Vec<Entry>,
    next: usize,
    filled: usize,
    seen: HashMap<u64, usize>,
}

/// Remembers the most recently inserted distinct 64-bit keys in a fixed FIFO.
/// Hits refresh neither insertion order nor age. Hashing happens outside the lock.
/// Share one `Arc<DedupSet>` across subscribers to collapse copies across subjects.
/// A 64-bit collision can suppress distinct content; this is not content validation.
/// Do not mix custom keys and payload hashes unless they share the same key space.
pub struct DedupSet {
    hash: RapidSecrets,
    entries: usize,
    window: Duration,
    base: Instant,
    state: Mutex<State>,
}

impl DedupSet {
    /// Zero entries selects 4096; a zero window disables age expiry.
    pub fn new(entries: usize, window: Duration) -> Self {
        let entries = if entries == 0 {
            DEFAULT_ENTRIES
        } else {
            entries
        };
        Self {
            // Randomize both seed and secrets; fixed-secret variants permit trivial
            // seed-independent collisions. The entire payload is hashed.
            hash: RapidSecrets::seed(rand::random()),
            entries,
            window,
            base: Instant::now(),
            state: Mutex::new(State {
                ring: vec![Entry::default(); entries],
                next: 0,
                filled: 0,
                seen: HashMap::with_capacity(entries),
            }),
        }
    }

    pub fn entries(&self) -> usize {
        self.entries
    }
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Reports whether these bytes are already present, inserting them on a miss.
    /// Unlike the default subscriber dispatch, this method also records empty bytes.
    pub fn seen(&self, data: &[u8]) -> bool {
        self.seen_key(rapidhash_v3_seeded(data, &self.hash))
    }

    /// Reports whether a caller-computed key is present, inserting it on a miss.
    pub fn seen_key(&self, key: u64) -> bool {
        let now = if self.window.is_zero() {
            0
        } else {
            self.base
                .elapsed()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX)
        };
        self.seen_key_at(key, now)
    }

    fn seen_key_at(&self, key: u64, now: u64) -> bool {
        let mut st = self.state.lock().unwrap();
        if let Some(&slot) = st.seen.get(&key) {
            if self.window.is_zero()
                || Duration::from_nanos(now.saturating_sub(st.ring[slot].at)) <= self.window
            {
                return true;
            }
        }
        let slot = st.next;
        if st.filled == self.entries {
            let old = st.ring[slot].key;
            // An expired key may already have been reinserted in a newer slot.
            if st.seen.get(&old) == Some(&slot) {
                st.seen.remove(&old);
            }
        } else {
            st.filled += 1;
        }
        st.ring[slot] = Entry { key, at: now };
        st.seen.insert(key, slot);
        st.next = (slot + 1) % self.entries;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    #[test]
    fn defaults_distinct_payloads_and_fifo_hits_do_not_refresh() {
        let defaults = DedupSet::new(0, Duration::ZERO);
        assert_eq!(defaults.entries(), 4096);
        assert_eq!(defaults.window(), Duration::ZERO);
        assert!(!defaults.seen(b""));
        assert!(defaults.seen(b""));
        let d = DedupSet::new(2, Duration::ZERO);
        assert!(!d.seen(b"a"));
        assert!(!d.seen(b"b"));
        assert!(d.seen(b"a"));
        assert!(!d.seen(b"c"));
        assert!(!d.seen(b"a"));
    }

    #[test]
    fn whole_payload_participates_in_key() {
        let d = DedupSet::new(128, Duration::ZERO);
        let data = vec![0x5a; 128 * 1024];
        assert!(!d.seen(&data));
        for offset in [0, 8, 16, 4096, data.len() / 2, data.len() - 1] {
            let mut changed = data.clone();
            changed[offset] ^= 1;
            assert!(!d.seen(&changed), "offset {offset} must not be ignored");
            assert!(d.seen(&changed));
        }
        assert!(d.seen(&data));
    }

    #[test]
    fn expiry_is_strict_and_stale_ring_slots_keep_new_mapping() {
        let d = DedupSet::new(3, Duration::from_nanos(10));
        assert!(!d.seen_key_at(1, 0));
        assert!(d.seen_key_at(1, 10));
        assert!(!d.seen_key_at(1, 11)); // same key, now in slot 1
        assert!(!d.seen_key_at(2, 12));
        assert!(!d.seen_key_at(3, 13)); // overwrite stale slot 0
        assert!(d.seen_key_at(1, 14));
        assert!(!d.seen_key_at(1, 22)); // hits did not refresh time
    }

    #[test]
    fn concurrent_first_copy_wins_once() {
        let d = Arc::new(DedupSet::new(4096, Duration::ZERO));
        let first = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let (d, first, barrier) = (d.clone(), first.clone(), barrier.clone());
                scope.spawn(move || {
                    barrier.wait();
                    for key in 0..2000 {
                        if !d.seen_key(key) {
                            first.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(first.load(Ordering::Relaxed), 2000);
    }
}
