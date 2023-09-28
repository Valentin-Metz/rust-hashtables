use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

pub struct HashMap<K: Hash + Eq, V> {
    buckets: RwLock<Vec<RwLock<Option<Entry<K, V>>>>>,
    length: Arc<AtomicUsize>,
    load_factor: f64,
}

struct Entry<K: Hash + Eq, V> {
    key: K,
    value: Arc<V>,
    next: Option<Box<Entry<K, V>>>,
}

impl<K: Hash + Eq, V> HashMap<K, V> {
    pub fn new() -> Self {
        Self::with_exact_capacity(0, 0.4)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_exact_capacity(capacity * 8, 0.4)
    }
    pub fn with_load_factor(load_factor: f64) -> Self {
        Self::with_exact_capacity(0, load_factor)
    }
    fn with_exact_capacity(capacity: usize, load_factor: f64) -> Self {
        Self {
            buckets: RwLock::new((0..capacity).map(|_| RwLock::new(None)).collect()),
            length: Arc::new(AtomicUsize::new(0)),
            load_factor,
        }
    }
    pub fn len(&self) -> usize {
        self.length.load(SeqCst)
    }
    pub fn is_empty(&self) -> bool {
        self.length.load(SeqCst) == 0
    }
    pub fn fill_factor(&self) -> f64 {
        let buckets = self.buckets.read();
        if buckets.is_empty() {
            0.0
        } else {
            self.length.load(SeqCst) as f64 / buckets.len() as f64
        }
    }
    pub fn clear(&self) {
        let mut buckets = self.buckets.write();
        self.length.store(0, SeqCst);
        for element in buckets.iter_mut() {
            *element = RwLock::new(None);
        }
    }

    fn calculate_hash(key: &K) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    pub fn insert(&self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        if self.buckets.read().is_empty() {
            let mut buckets = self.buckets.write();
            if buckets.is_empty() {
                *buckets = (0..64).map(|_| RwLock::new(None)).collect();
            }
        }
        if self.fill_factor() >= self.load_factor {
            self.rehash();
        }
        let buckets = self.buckets.read();
        let old = HashMap::pre_locked_remove(&buckets, &self.length, &key);
        let hash = Self::calculate_hash(&key);
        let index = hash as usize % buckets.len();
        let entry = Entry {
            key,
            value,
            next: None,
        };

        let mut bucket = buckets[index].write();
        match &mut *bucket {
            Some(first_entry) => {
                let next = mem::replace(first_entry, entry);
                first_entry.next = Some(Box::new(next));
            }
            None => {
                *bucket = Some(entry);
            }
        }
        self.length.fetch_add(1, SeqCst);
        old
    }

    fn rehash(&self) {
        let buckets = &mut *self.buckets.write();
        if (self.length.load(SeqCst) as f64 / buckets.len() as f64) < self.load_factor {
            return;
        }
        let new_table: HashMap<K, V> =
            HashMap::with_exact_capacity(buckets.len() * 2, self.load_factor);
        for bucket in buckets.iter() {
            let bucket = &mut *bucket.write();
            if let Some(entry) = bucket.take() {
                new_table.insert(entry.key, entry.value);
                let mut current = entry.next;
                while let Some(entry) = current {
                    new_table.insert(entry.key, entry.value);
                    current = entry.next;
                }
            }
        }
        let new_buckets = &mut *new_table.buckets.write();
        mem::swap(buckets, new_buckets);
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let buckets = &*self.buckets.read();
        if buckets.is_empty() {
            return None;
        }
        let hash = Self::calculate_hash(key);
        let index = hash as usize % buckets.len();

        let result = match &*buckets[index].read() {
            Some(bucket) => {
                // First bucket is a hit
                if bucket.key == *key {
                    return Some(bucket.value.clone());
                }
                // First bucket is a miss and has next
                let mut current = &bucket.next;
                loop {
                    match current {
                        // Entry located
                        Some(entry) if entry.key == *key => {
                            return Some(entry.value.clone());
                        }
                        // Cycle through the linked list
                        Some(entry) => {
                            current = &entry.next;
                        }
                        None => {
                            return None;
                        }
                    }
                }
            }
            None => None,
        };
        result
    }

    pub fn remove(&self, key: &K) -> Option<Arc<V>> {
        let buckets = self.buckets.read();
        HashMap::pre_locked_remove(&*buckets, &self.length, key)
    }
    fn pre_locked_remove(
        buckets: &Vec<RwLock<Option<Entry<K, V>>>>,
        length: &AtomicUsize,
        key: &K,
    ) -> Option<Arc<V>> {
        if buckets.is_empty() {
            return None;
        }
        let hash = Self::calculate_hash(key);
        let index = hash as usize % buckets.len();

        let entry = &mut *buckets[index].write();
        match entry {
            Some(bucket) => {
                match &mut bucket.next {
                    // First bucket is a hit and has no next
                    None if bucket.key == *key => {
                        let result = entry.take().unwrap();
                        length.fetch_sub(1, SeqCst);
                        Some(result.value)
                    }
                    // Fist bucket is a hit and has next
                    Some(_next) if bucket.key == *key => {
                        let result = entry.take().unwrap();
                        *entry = Some(*result.next.unwrap());
                        length.fetch_sub(1, SeqCst);
                        Some(result.value)
                    }
                    // First bucket is a miss and has next
                    Some(miss) => {
                        let mut current = &mut miss.next;
                        loop {
                            match current {
                                // Entry located
                                Some(entry) if entry.key == *key => {
                                    let mut result = current.take().unwrap();
                                    *current = result.next.take();
                                    length.fetch_sub(1, SeqCst);
                                    return Some(result.value);
                                }
                                // Cycle through the linked list
                                Some(entry) => {
                                    current = &mut entry.next;
                                }
                                None => {
                                    return None;
                                }
                            }
                        }
                    }
                    // First bucket is a miss and has no next
                    None => None,
                }
            }
            None => None,
        }
    }
}

impl<K: Hash + Eq, V> Default for HashMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_new() {
        let table: HashMap<i32, i32> = HashMap::new();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.fill_factor(), 0.0);
    }

    #[test]
    fn test_with_capacity() {
        let table: HashMap<i32, i32> = HashMap::with_capacity(10);
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.fill_factor(), 0.0);
    }

    #[test]
    fn test_insert() {
        let table = HashMap::with_exact_capacity(8, 0.75);
        assert_eq!(table.insert(1, Arc::new(10)), None);
        assert_eq!(table.len(), 1);
        assert!(!table.is_empty());
        assert_eq!(table.fill_factor(), 0.125);
        assert_eq!(table.insert(1, Arc::new(20)), Some(Arc::new(10)));
        assert_eq!(table.len(), 1);
        assert_eq!(table.fill_factor(), 0.125);
        assert_eq!(table.insert(2, Arc::new(30)), None);
        assert_eq!(table.len(), 2);
        assert_eq!(table.fill_factor(), 0.25);
    }

    #[test]
    fn test_get() {
        let table = HashMap::new();
        table.insert(1, Arc::new(10));
        assert_eq!(table.get(&1), Some(Arc::new(10)));
        assert_eq!(table.get(&2), None);
        table.insert(2, Arc::new(20));
        assert_eq!(table.get(&1), Some(Arc::new(10)));
        assert_eq!(table.get(&2), Some(Arc::new(20)));
    }

    #[test]
    fn test_remove() {
        let table = HashMap::new();
        table.insert(1, Arc::new(10));
        assert_eq!(table.remove(&2), None);
        assert_eq!(table.remove(&1), Some(Arc::new(10)));
        assert_eq!(table.len(), 0);
        assert_eq!(table.fill_factor(), 0.0);
        table.insert(1, Arc::new(20));
        table.insert(2, Arc::new(30));
        assert_eq!(table.remove(&1), Some(Arc::new(20)));
        assert_eq!(table.len(), 1);
        assert_eq!(table.fill_factor(), 0.015625);
        assert_eq!(table.remove(&2), Some(Arc::new(30)));
        assert_eq!(table.len(), 0);
        assert_eq!(table.fill_factor(), 0.0);
    }

    #[test]
    fn test_insert_multiple_entries() {
        let hash_table = HashMap::new();

        hash_table.insert(1, Arc::new("one"));
        hash_table.insert(2, Arc::new("two"));
        hash_table.insert(3, Arc::new("three"));
        hash_table.insert(4, Arc::new("four"));

        assert_eq!(hash_table.len(), 4);
        assert_eq!(hash_table.get(&1), Some(Arc::new("one")));
        assert_eq!(hash_table.get(&2), Some(Arc::new("two")));
        assert_eq!(hash_table.get(&3), Some(Arc::new("three")));
        assert_eq!(hash_table.get(&4), Some(Arc::new("four")));
    }

    #[test]
    fn test_get_non_existing_key() {
        let hash_table: HashMap<&str, u128> = HashMap::new();
        assert_eq!(hash_table.get(&"non-existing"), None);
    }

    #[test]
    fn test_remove_existing_key() {
        let hash_table = HashMap::new();

        hash_table.insert(1, Arc::new("one"));
        hash_table.insert(2, Arc::new("two"));
        hash_table.insert(3, Arc::new("three"));

        let removed = hash_table.remove(&2);

        assert_eq!(hash_table.len(), 2);
        assert_eq!(removed, Some(Arc::new("two")));
        assert_eq!(hash_table.get(&1), Some(Arc::new("one")));
        assert_eq!(hash_table.get(&2), None);
        assert_eq!(hash_table.get(&3), Some(Arc::new("three")));
    }

    #[test]
    fn test_remove_non_existing_key() {
        let hash_table = HashMap::new();

        hash_table.insert(1, Arc::new("one"));
        hash_table.insert(2, Arc::new("two"));
        hash_table.insert(3, Arc::new("three"));

        let removed = hash_table.remove(&4);

        assert_eq!(hash_table.len(), 3);
        assert_eq!(removed, None);
        assert_eq!(hash_table.get(&1), Some(Arc::new("one")));
        assert_eq!(hash_table.get(&2), Some(Arc::new("two")));
        assert_eq!(hash_table.get(&3), Some(Arc::new("three")));
    }

    #[test]
    fn test_collision_handling() {
        let table = HashMap::with_exact_capacity(2, 1.0);
        table.insert(1, Arc::new("one"));
        table.insert(2, Arc::new("two"));
        table.insert(3, Arc::new("three"));
        assert_eq!(table.len(), 3);
        assert_eq!(table.get(&1), Some(Arc::new("one")));
        assert_eq!(table.get(&2), Some(Arc::new("two")));
        assert_eq!(table.get(&3), Some(Arc::new("three")));
    }

    #[test]
    fn test_rehash() {
        let table = HashMap::with_exact_capacity(4, 1.0);
        table.insert(1, Arc::new("one"));
        table.insert(2, Arc::new("two"));
        table.insert(3, Arc::new("three"));
        table.insert(4, Arc::new("four"));
        table.insert(5, Arc::new("five"));
        table.insert(6, Arc::new("six"));
        assert_eq!(table.len(), 6);
        assert_eq!(table.get(&1), Some(Arc::new("one")));
        assert_eq!(table.get(&2), Some(Arc::new("two")));
        assert_eq!(table.get(&3), Some(Arc::new("three")));
        assert_eq!(table.get(&4), Some(Arc::new("four")));
        assert_eq!(table.get(&5), Some(Arc::new("five")));
        assert_eq!(table.get(&6), Some(Arc::new("six")));
        assert!(table.fill_factor() < 1.0);
    }

    #[test]
    fn test_insert_overwrite() {
        let table = HashMap::new();
        table.insert(1, Arc::new("one"));
        table.insert(1, Arc::new("new_one"));
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&1), Some(Arc::new("new_one")));
    }

    #[test]
    fn test_insert_negative_keys() {
        let table = HashMap::new();
        table.insert(-1, Arc::new("minus_one"));
        table.insert(-2, Arc::new("minus_two"));
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&-1), Some(Arc::new("minus_one")));
        assert_eq!(table.get(&-2), Some(Arc::new("minus_two")));
    }

    #[test]
    fn test_insert_large_keys() {
        let table = HashMap::new();
        table.insert(u128::MAX, Arc::new("max_key"));
        table.insert(u128::MIN, Arc::new("min_key"));
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&u128::MAX), Some(Arc::new("max_key")));
        assert_eq!(table.get(&u128::MIN), Some(Arc::new("min_key")));
    }

    #[test]
    fn test_insert_large_values() {
        let table = HashMap::new();
        table.insert(1, Arc::new(u64::MAX));
        table.insert(2, Arc::new(u64::MIN));
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&1), Some(Arc::new(u64::MAX)));
        assert_eq!(table.get(&2), Some(Arc::new(u64::MIN)));
    }

    #[test]
    fn test_remove_from_empty_table() {
        let table: HashMap<i32, i32> = HashMap::new();
        assert_eq!(table.remove(&1), None);
    }

    #[test]
    fn test_clear() {
        let table = HashMap::new();
        table.insert(1, Arc::new("one"));
        table.insert(2, Arc::new("two"));
        table.clear();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.get(&1), None);
        assert_eq!(table.get(&2), None);
    }

    #[test]
    fn multithreaded_test() {
        let table = Arc::new(HashMap::new());
        let mut threads = Vec::new();
        for i in 0..1000 {
            let table = table.clone();
            threads.push(thread::spawn(move || {
                table.insert(i, Arc::new(i));
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(table.len(), 1000);
        for i in 0..1000 {
            assert_eq!(table.get(&i), Some(Arc::new(i)));
        }
    }
}
