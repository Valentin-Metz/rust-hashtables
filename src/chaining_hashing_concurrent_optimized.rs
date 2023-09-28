use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

pub struct HashMap<K: Hash + Eq, V> {
    buckets: Vec<RwLock<Option<Entry<K, V>>>>,
}

struct Entry<K: Hash + Eq, V> {
    key: K,
    value: Arc<V>,
    next: Option<Box<Entry<K, V>>>,
}

impl<K: Hash + Eq, V> HashMap<K, V> {
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self::with_exact_capacity(capacity * 8)
    }
    fn with_exact_capacity(capacity: usize) -> Self {
        Self {
            buckets: (0..capacity).map(|_| RwLock::new(None)).collect(),
        }
    }
    pub fn clear(&self) {
        for element in self.buckets.iter() {
            *element.write() = None;
        }
    }

    fn calculate_hash(key: &K) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    pub fn insert(&self, key: K, value: Arc<V>) -> Option<Arc<V>> {
        let old = self.remove(&key);
        let hash = Self::calculate_hash(&key);
        let index = hash as usize % self.buckets.len();
        let entry = Entry {
            key,
            value,
            next: None,
        };

        let mut bucket = self.buckets[index].write();
        match &mut *bucket {
            Some(first_entry) => {
                let next = mem::replace(first_entry, entry);
                first_entry.next = Some(Box::new(next));
            }
            None => {
                *bucket = Some(entry);
            }
        }
        old
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let hash = Self::calculate_hash(key);
        let index = hash as usize % self.buckets.len();

        let result = match &*self.buckets[index].read() {
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
        let hash = Self::calculate_hash(key);
        let index = hash as usize % self.buckets.len();

        let entry = &mut *self.buckets[index].write();
        match entry {
            Some(bucket) => {
                match &mut bucket.next {
                    // First bucket is a hit and has no next
                    None if bucket.key == *key => {
                        let result = entry.take().unwrap();
                        Some(result.value)
                    }
                    // Fist bucket is a hit and has next
                    Some(_next) if bucket.key == *key => {
                        let result = entry.take().unwrap();
                        *entry = Some(*result.next.unwrap());
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_with_capacity() {
        HashMap::<u128, u128>::with_capacity(10);
    }

    #[test]
    fn test_insert() {
        let table = HashMap::with_exact_capacity(8);
        assert_eq!(table.insert(1, Arc::new(10)), None);
        assert_eq!(table.insert(1, Arc::new(20)), Some(Arc::new(10)));
        assert_eq!(table.insert(2, Arc::new(30)), None);
    }

    #[test]
    fn test_get() {
        let table = HashMap::with_capacity(8);
        table.insert(1, Arc::new(10));
        assert_eq!(table.get(&1), Some(Arc::new(10)));
        assert_eq!(table.get(&2), None);
        table.insert(2, Arc::new(20));
        assert_eq!(table.get(&1), Some(Arc::new(10)));
        assert_eq!(table.get(&2), Some(Arc::new(20)));
    }

    #[test]
    fn test_remove() {
        let table = HashMap::with_capacity(64);
        table.insert(1, Arc::new(10));
        assert_eq!(table.remove(&2), None);
        assert_eq!(table.remove(&1), Some(Arc::new(10)));
        table.insert(1, Arc::new(20));
        table.insert(2, Arc::new(30));
        assert_eq!(table.remove(&1), Some(Arc::new(20)));
        assert_eq!(table.remove(&2), Some(Arc::new(30)));
    }

    #[test]
    fn test_insert_multiple_entries() {
        let hash_table = HashMap::with_capacity(64);

        hash_table.insert(1, Arc::new("one"));
        hash_table.insert(2, Arc::new("two"));
        hash_table.insert(3, Arc::new("three"));
        hash_table.insert(4, Arc::new("four"));

        assert_eq!(hash_table.get(&1), Some(Arc::new("one")));
        assert_eq!(hash_table.get(&2), Some(Arc::new("two")));
        assert_eq!(hash_table.get(&3), Some(Arc::new("three")));
        assert_eq!(hash_table.get(&4), Some(Arc::new("four")));
    }

    #[test]
    fn test_get_non_existing_key() {
        let hash_table: HashMap<&str, u128> = HashMap::with_capacity(1);
        assert_eq!(hash_table.get(&"non-existing"), None);
    }

    #[test]
    fn test_remove_existing_key() {
        let hash_table = HashMap::with_capacity(4);

        hash_table.insert(1, Arc::new("one"));
        hash_table.insert(2, Arc::new("two"));
        hash_table.insert(3, Arc::new("three"));

        let removed = hash_table.remove(&2);

        assert_eq!(removed, Some(Arc::new("two")));
        assert_eq!(hash_table.get(&1), Some(Arc::new("one")));
        assert_eq!(hash_table.get(&2), None);
        assert_eq!(hash_table.get(&3), Some(Arc::new("three")));
    }

    #[test]
    fn test_remove_non_existing_key() {
        let hash_table = HashMap::with_capacity(2);

        hash_table.insert(1, Arc::new("one"));
        hash_table.insert(2, Arc::new("two"));
        hash_table.insert(3, Arc::new("three"));

        let removed = hash_table.remove(&4);

        assert_eq!(removed, None);
        assert_eq!(hash_table.get(&1), Some(Arc::new("one")));
        assert_eq!(hash_table.get(&2), Some(Arc::new("two")));
        assert_eq!(hash_table.get(&3), Some(Arc::new("three")));
    }

    #[test]
    fn test_collision_handling() {
        let table = HashMap::with_exact_capacity(2);
        table.insert(1, Arc::new("one"));
        table.insert(2, Arc::new("two"));
        table.insert(3, Arc::new("three"));
        assert_eq!(table.get(&1), Some(Arc::new("one")));
        assert_eq!(table.get(&2), Some(Arc::new("two")));
        assert_eq!(table.get(&3), Some(Arc::new("three")));
    }

    #[test]
    fn test_rehash() {
        let table = HashMap::with_exact_capacity(4);
        table.insert(1, Arc::new("one"));
        table.insert(2, Arc::new("two"));
        table.insert(3, Arc::new("three"));
        table.insert(4, Arc::new("four"));
        table.insert(5, Arc::new("five"));
        table.insert(6, Arc::new("six"));
        assert_eq!(table.get(&1), Some(Arc::new("one")));
        assert_eq!(table.get(&2), Some(Arc::new("two")));
        assert_eq!(table.get(&3), Some(Arc::new("three")));
        assert_eq!(table.get(&4), Some(Arc::new("four")));
        assert_eq!(table.get(&5), Some(Arc::new("five")));
        assert_eq!(table.get(&6), Some(Arc::new("six")));
    }

    #[test]
    fn test_insert_overwrite() {
        let table = HashMap::with_capacity(256);
        table.insert(1, Arc::new("one"));
        table.insert(1, Arc::new("new_one"));
        assert_eq!(table.get(&1), Some(Arc::new("new_one")));
    }

    #[test]
    fn test_insert_negative_keys() {
        let table = HashMap::with_capacity(512);
        table.insert(-1, Arc::new("minus_one"));
        table.insert(-2, Arc::new("minus_two"));
        assert_eq!(table.get(&-1), Some(Arc::new("minus_one")));
        assert_eq!(table.get(&-2), Some(Arc::new("minus_two")));
    }

    #[test]
    fn test_insert_large_keys() {
        let table = HashMap::with_capacity(1024);
        table.insert(u128::MAX, Arc::new("max_key"));
        table.insert(u128::MIN, Arc::new("min_key"));
        assert_eq!(table.get(&u128::MAX), Some(Arc::new("max_key")));
        assert_eq!(table.get(&u128::MIN), Some(Arc::new("min_key")));
    }

    #[test]
    fn test_insert_large_values() {
        let table = HashMap::with_capacity(1);
        table.insert(1, Arc::new(u64::MAX));
        table.insert(2, Arc::new(u64::MIN));
        assert_eq!(table.get(&1), Some(Arc::new(u64::MAX)));
        assert_eq!(table.get(&2), Some(Arc::new(u64::MIN)));
    }

    #[test]
    fn test_remove_from_empty_table() {
        let table: HashMap<i32, i32> = HashMap::with_capacity(1);
        assert_eq!(table.remove(&1), None);
    }

    #[test]
    fn test_clear() {
        let table = HashMap::with_capacity(2);
        table.insert(1, Arc::new("one"));
        table.insert(2, Arc::new("two"));
        table.clear();
        assert_eq!(table.get(&1), None);
        assert_eq!(table.get(&2), None);
    }

    #[test]
    #[should_panic(expected = "assertion failed: capacity > 0")]
    fn forbid_zero_capacity() {
        HashMap::<i32, i32>::with_capacity(0);
    }

    #[test]
    fn multithreaded_test() {
        let table = Arc::new(HashMap::with_capacity(1024));
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
        for i in 0..1000 {
            assert_eq!(table.get(&i), Some(Arc::new(i)));
        }
    }
}
