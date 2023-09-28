use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

pub struct HashMap<K: Hash + Eq, V> {
    buckets: Vec<Option<Entry<K, V>>>,
    length: usize,
    load_factor: f64,
}

struct Entry<K: Hash + Eq, V> {
    key: K,
    value: V,
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
            buckets: (0..capacity).map(|_| None).collect(),
            length: 0,
            load_factor,
        }
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    pub fn fill_factor(&self) -> f64 {
        if self.buckets.is_empty() {
            0.0
        } else {
            self.length as f64 / self.buckets.len() as f64
        }
    }
    pub fn clear(&mut self) {
        self.length = 0;
        for element in self.buckets.iter_mut() {
            *element = None;
        }
    }

    fn calculate_hash(key: &K) -> u64 {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        s.finish()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.buckets.is_empty() {
            self.buckets = (0..64).map(|_| None).collect();
        }
        if self.fill_factor() >= self.load_factor {
            self.rehash();
        }
        let old = self.remove(&key);
        let hash = Self::calculate_hash(&key);
        let index = hash as usize % self.buckets.len();
        let entry = Entry {
            key,
            value,
            next: None,
        };
        match self.buckets.get_mut(index) {
            Some(option) => match option {
                Some(bucket) => {
                    let next = mem::replace(bucket, entry);
                    bucket.next = Some(Box::new(next));
                }
                None => {
                    *option = Some(entry);
                }
            },
            _ => {
                unreachable!("index out of bounds");
            }
        }
        self.length += 1;
        old
    }

    fn rehash(&mut self) {
        let mut new_table = HashMap::with_exact_capacity(self.buckets.len() * 2, self.load_factor);
        for bucket in self.buckets.iter_mut() {
            if let Some(entry) = bucket.take() {
                new_table.insert(entry.key, entry.value);
                let mut current = entry.next;
                while let Some(entry) = current {
                    new_table.insert(entry.key, entry.value);
                    current = entry.next;
                }
            }
        }
        mem::swap(self, &mut new_table);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.is_empty() {
            return None;
        }
        let hash = Self::calculate_hash(key);
        let index = hash as usize % self.buckets.len();
        match self.buckets.get(index) {
            Some(option) => {
                match option {
                    Some(bucket) => {
                        // First bucket is a hit
                        if bucket.key == *key {
                            return Some(&bucket.value);
                        }
                        // First bucket is a miss and has next
                        let mut current = &bucket.next;
                        loop {
                            match current {
                                // Entry located
                                Some(entry) if entry.key == *key => {
                                    return Some(&entry.value);
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
                }
            }
            _ => {
                unreachable!("index out of bounds");
            }
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.is_empty() {
            return None;
        }
        let hash = Self::calculate_hash(key);
        let index = hash as usize % self.buckets.len();
        match self.buckets.get_mut(index) {
            Some(option) => {
                match option {
                    Some(bucket) => {
                        // First bucket is a hit
                        if bucket.key == *key {
                            return Some(&mut bucket.value);
                        }
                        // First bucket is a miss and has next
                        let mut current = &mut bucket.next;
                        loop {
                            match current {
                                Some(entry) => {
                                    // Entry located
                                    if entry.key == *key {
                                        return Some(&mut entry.value);
                                    }
                                    // Cycle through the linked list
                                    current = &mut entry.next;
                                }
                                None => {
                                    return None;
                                }
                            }
                        }
                    }
                    None => None,
                }
            }
            _ => {
                unreachable!("index out of bounds");
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_empty() {
            return None;
        }
        let hash = Self::calculate_hash(key);
        let index = hash as usize % self.buckets.len();
        match self.buckets.get_mut(index) {
            Some(option) => {
                match option {
                    Some(bucket) => {
                        match &mut bucket.next {
                            // First bucket is a hit and has no next
                            None if bucket.key == *key => {
                                let result = option.take().unwrap();
                                self.length -= 1;
                                Some(result.value)
                            }
                            // Fist bucket is a hit and has next
                            Some(_next) if bucket.key == *key => {
                                let result = option.take().unwrap();
                                *option = Some(*result.next.unwrap());
                                self.length -= 1;
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
                                            self.length -= 1;
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
            _ => {
                unreachable!("index out of bounds");
            }
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
        let mut table = HashMap::with_exact_capacity(8, 0.75);
        assert_eq!(table.insert(1, 10), None);
        assert_eq!(table.len(), 1);
        assert!(!table.is_empty());
        assert_eq!(table.fill_factor(), 0.125);
        assert_eq!(table.insert(1, 20), Some(10));
        assert_eq!(table.len(), 1);
        assert_eq!(table.fill_factor(), 0.125);
        assert_eq!(table.insert(2, 30), None);
        assert_eq!(table.len(), 2);
        assert_eq!(table.fill_factor(), 0.25);
    }

    #[test]
    fn test_get() {
        let mut table = HashMap::new();
        table.insert(1, 10);
        assert_eq!(table.get(&1), Some(&10));
        assert_eq!(table.get(&2), None);
        table.insert(2, 20);
        assert_eq!(table.get(&1), Some(&10));
        assert_eq!(table.get(&2), Some(&20));
    }

    #[test]
    fn test_get_mut() {
        let mut table = HashMap::new();
        table.insert(1, 10);
        assert_eq!(table.get_mut(&1), Some(&mut 10));
        assert_eq!(table.get_mut(&2), None);
        table.insert(2, 20);
        assert_eq!(table.get_mut(&1), Some(&mut 10));
        assert_eq!(table.get_mut(&2), Some(&mut 20));
        *table.get_mut(&1).unwrap() = 30;
        assert_eq!(table.get(&1), Some(&30));
    }

    #[test]
    fn test_remove() {
        let mut table = HashMap::new();
        table.insert(1, 10);
        assert_eq!(table.remove(&2), None);
        assert_eq!(table.remove(&1), Some(10));
        assert_eq!(table.len(), 0);
        assert_eq!(table.fill_factor(), 0.0);
        table.insert(1, 20);
        table.insert(2, 30);
        assert_eq!(table.remove(&1), Some(20));
        assert_eq!(table.len(), 1);
        assert_eq!(table.fill_factor(), 0.015625);
        assert_eq!(table.remove(&2), Some(30));
        assert_eq!(table.len(), 0);
        assert_eq!(table.fill_factor(), 0.0);
    }

    #[test]
    fn test_insert_multiple_entries() {
        let mut hash_table = HashMap::new();

        hash_table.insert(1, "one");
        hash_table.insert(2, "two");
        hash_table.insert(3, "three");
        hash_table.insert(4, "four");

        assert_eq!(hash_table.len(), 4);
        assert_eq!(hash_table.get(&1), Some(&"one"));
        assert_eq!(hash_table.get(&2), Some(&"two"));
        assert_eq!(hash_table.get(&3), Some(&"three"));
        assert_eq!(hash_table.get(&4), Some(&"four"));
    }

    #[test]
    fn test_get_non_existing_key() {
        let hash_table: HashMap<&str, u128> = HashMap::new();
        assert_eq!(hash_table.get(&"non-existing"), None);
    }

    #[test]
    fn test_remove_existing_key() {
        let mut hash_table = HashMap::new();

        hash_table.insert(1, "one");
        hash_table.insert(2, "two");
        hash_table.insert(3, "three");

        let removed = hash_table.remove(&2);

        assert_eq!(hash_table.len(), 2);
        assert_eq!(removed, Some("two"));
        assert_eq!(hash_table.get(&1), Some(&"one"));
        assert_eq!(hash_table.get(&2), None);
        assert_eq!(hash_table.get(&3), Some(&"three"));
    }

    #[test]
    fn test_remove_non_existing_key() {
        let mut hash_table = HashMap::new();

        hash_table.insert(1, "one");
        hash_table.insert(2, "two");
        hash_table.insert(3, "three");

        let removed = hash_table.remove(&4);

        assert_eq!(hash_table.len(), 3);
        assert_eq!(removed, None);
        assert_eq!(hash_table.get(&1), Some(&"one"));
        assert_eq!(hash_table.get(&2), Some(&"two"));
        assert_eq!(hash_table.get(&3), Some(&"three"));
    }

    #[test]
    fn test_collision_handling() {
        let mut table = HashMap::with_exact_capacity(2, 1.0);
        table.insert(1, "one");
        table.insert(2, "two");
        table.insert(3, "three");
        assert_eq!(table.len(), 3);
        assert_eq!(table.get(&1), Some(&"one"));
        assert_eq!(table.get(&2), Some(&"two"));
        assert_eq!(table.get(&3), Some(&"three"));
    }

    #[test]
    fn test_rehash() {
        let mut table = HashMap::with_exact_capacity(4, 1.0);
        table.insert(1, "one");
        table.insert(2, "two");
        table.insert(3, "three");
        table.insert(4, "four");
        table.insert(5, "five");
        table.insert(6, "six");
        assert_eq!(table.len(), 6);
        assert_eq!(table.get(&1), Some(&"one"));
        assert_eq!(table.get(&2), Some(&"two"));
        assert_eq!(table.get(&3), Some(&"three"));
        assert_eq!(table.get(&4), Some(&"four"));
        assert_eq!(table.get(&5), Some(&"five"));
        assert_eq!(table.get(&6), Some(&"six"));
        assert!(table.fill_factor() < 1.0);
    }

    #[test]
    fn test_insert_overwrite() {
        let mut table = HashMap::new();
        table.insert(1, "one");
        table.insert(1, "new_one");
        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&1), Some(&"new_one"));
    }

    #[test]
    fn test_insert_negative_keys() {
        let mut table = HashMap::new();
        table.insert(-1, "minus_one");
        table.insert(-2, "minus_two");
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&-1), Some(&"minus_one"));
        assert_eq!(table.get(&-2), Some(&"minus_two"));
    }

    #[test]
    fn test_insert_large_keys() {
        let mut table = HashMap::new();
        table.insert(u128::MAX, "max_key");
        table.insert(u128::MIN, "min_key");
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&u128::MAX), Some(&"max_key"));
        assert_eq!(table.get(&u128::MIN), Some(&"min_key"));
    }

    #[test]
    fn test_insert_large_values() {
        let mut table = HashMap::new();
        table.insert(1, u64::MAX);
        table.insert(2, u64::MIN);
        assert_eq!(table.len(), 2);
        assert_eq!(table.get(&1), Some(&u64::MAX));
        assert_eq!(table.get(&2), Some(&u64::MIN));
    }

    #[test]
    fn test_empty_get_mut() {
        let mut table: HashMap<i32, i32> = HashMap::new();
        assert_eq!(table.get_mut(&1), None);
    }

    #[test]
    fn test_remove_from_empty_table() {
        let mut table: HashMap<i32, i32> = HashMap::new();
        assert_eq!(table.remove(&1), None);
    }

    #[test]
    fn test_clear() {
        let mut table = HashMap::new();
        table.insert(1, "one");
        table.insert(2, "two");
        table.clear();
        assert_eq!(table.len(), 0);
        assert!(table.is_empty());
        assert_eq!(table.get(&1), None);
        assert_eq!(table.get(&2), None);
    }
}
