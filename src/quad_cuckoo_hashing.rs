use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

use rand::seq::IteratorRandom;
use rand::Rng;

// Cuckoo hashing with an arbitrary amount of hash functions and linear probing
pub struct HashMap<K: Hash + Eq, V> {
    buckets: Vec<Vec<Option<Entry<K, V>>>>,
    bucket_size: usize,
    hasher_vec: Vec<DefaultHasher>,
    load_factor: f64,
    length: usize,
}

struct Entry<K: Hash + Eq, V> {
    key: K,
    value: V,
}

impl<K: Hash + Eq, V> HashMap<K, V> {
    pub fn new() -> Self {
        Self::with_exact_capacity(0, 4, 4, 0.8)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_exact_capacity(capacity * 16, 4, 4, 0.8)
    }
    pub fn with_load_factor(fill_factor: f64) -> Self {
        Self::with_exact_capacity(0, 4, 4, fill_factor)
    }
    fn with_exact_capacity(
        capacity: usize,
        bucket_size: usize,
        hasher_amount: usize,
        load_factor: f64,
    ) -> Self {
        let mut rng = rand::thread_rng();
        assert!(capacity == 0 || capacity >= bucket_size * hasher_amount);
        assert_eq!(capacity % bucket_size, 0);
        assert_eq!(capacity % hasher_amount, 0);
        Self {
            buckets: (0..(capacity / bucket_size))
                .map(|_| (0..bucket_size).map(|_| None).collect())
                .collect(),
            bucket_size,
            hasher_vec: (0..hasher_amount)
                .map(|_| {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(rng.gen::<u64>());
                    hasher
                })
                .collect(),
            load_factor,
            length: 0,
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
            self.length as f64 / (self.buckets.len() * self.buckets[0].len()) as f64
        }
    }
    pub fn clear(&mut self) {
        self.length = 0;
        for element in self.buckets.iter_mut().flat_map(|bucket| bucket.iter_mut()) {
            *element = None;
        }
    }

    fn calculate_hash(key: &K, hasher: &DefaultHasher) -> u64 {
        let mut hasher = hasher.clone();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.buckets.is_empty() {
            self.buckets = (0..64)
                .map(|_| (0..self.bucket_size).map(|_| None).collect())
                .collect();
        }
        if self.fill_factor() >= self.load_factor {
            self.rehash(2);
        }
        let mut entry = Entry { key, value };

        debug_assert_eq!(self.buckets.len() % self.hasher_vec.len(), 0);
        let chunk_size = self.buckets.len() / self.hasher_vec.len();
        for _ in 0..self.length + 1 {
            // Replace duplicate entry if it exists
            if let Some(old_entry) = self
                .buckets
                .chunks_exact_mut(chunk_size)
                .zip(&self.hasher_vec)
                .flat_map(|buckets_for_hash_function| {
                    &mut buckets_for_hash_function.0[Self::calculate_hash(
                        &entry.key,
                        buckets_for_hash_function.1,
                    ) as usize
                        % buckets_for_hash_function.0.len()]
                })
                .flatten()
                .find(|e| e.key == entry.key)
            {
                return Some(mem::replace(&mut old_entry.value, entry.value));
            }
            // Insert entry into an empty spot
            else if let Some(empty_spot) = self
                .buckets
                .chunks_exact_mut(chunk_size)
                .zip(&self.hasher_vec)
                .flat_map(|buckets_for_hash_function| {
                    &mut buckets_for_hash_function.0[Self::calculate_hash(
                        &entry.key,
                        buckets_for_hash_function.1,
                    ) as usize
                        % buckets_for_hash_function.0.len()]
                })
                .find(|e| e.is_none())
            {
                self.length += 1;
                let replaced = mem::replace(empty_spot, Some(entry));
                debug_assert!(replaced.is_none());
                return None;
            }
            // Kick a random entry and replace it
            else if let Some(kicked_entry) = self
                .buckets
                .chunks_exact_mut(chunk_size)
                .zip(&self.hasher_vec)
                .flat_map(|buckets_for_hash_function| {
                    &mut buckets_for_hash_function.0[Self::calculate_hash(
                        &entry.key,
                        buckets_for_hash_function.1,
                    ) as usize
                        % buckets_for_hash_function.0.len()]
                })
                .flatten()
                .choose(&mut rand::thread_rng())
            {
                entry = mem::replace(kicked_entry, entry);
            }
        }
        self.rehash(1);
        self.insert(entry.key, entry.value)
    }

    fn rehash(&mut self, resize_factor: usize) {
        let mut new_table = HashMap::with_exact_capacity(
            self.buckets.len() * self.bucket_size * resize_factor,
            self.bucket_size,
            self.hasher_vec.len(),
            self.load_factor,
        );
        for entry in self.buckets.iter_mut().flatten() {
            if let Some(entry) = entry.take() {
                new_table.insert(entry.key, entry.value);
            }
        }
        mem::swap(self, &mut new_table);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.is_empty() {
            return None;
        }
        self.buckets
            .chunks_exact(self.buckets.len() / self.hasher_vec.len())
            .zip(&self.hasher_vec)
            .flat_map(|buckets_for_hash_function| {
                &buckets_for_hash_function.0[Self::calculate_hash(key, buckets_for_hash_function.1)
                    as usize
                    % buckets_for_hash_function.0.len()]
            })
            .flatten()
            .find(|e| e.key == *key)
            .map(|e| &e.value)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.is_empty() {
            return None;
        }
        let chunk_size = self.buckets.len() / self.hasher_vec.len();
        self.buckets
            .chunks_exact_mut(chunk_size)
            .zip(&self.hasher_vec)
            .flat_map(|buckets_for_hash_function| {
                &mut buckets_for_hash_function.0[Self::calculate_hash(
                    key,
                    buckets_for_hash_function.1,
                ) as usize
                    % buckets_for_hash_function.0.len()]
            })
            .flatten()
            .find(|e| e.key == *key)
            .map(|e| &mut e.value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_empty() {
            return None;
        }
        let chunk_size = self.buckets.len() / self.hasher_vec.len();
        self.buckets
            .chunks_exact_mut(chunk_size)
            .zip(&self.hasher_vec)
            .flat_map(|buckets_for_hash_function| {
                &mut buckets_for_hash_function.0[Self::calculate_hash(
                    key,
                    buckets_for_hash_function.1,
                ) as usize
                    % buckets_for_hash_function.0.len()]
            })
            .find_map(|e| match e {
                Some(entry) if entry.key == *key => {
                    self.length -= 1;
                    e.take()
                }
                _ => None,
            })
            .map(|e| e.value)
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
        let mut table = HashMap::with_exact_capacity(8, 2, 2, 0.5);
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
        let mut table = HashMap::with_exact_capacity(8, 2, 2, 0.5);
        table.insert(1, 10);
        assert_eq!(table.remove(&2), None);
        assert_eq!(table.remove(&1), Some(10));
        assert_eq!(table.len(), 0);
        assert_eq!(table.fill_factor(), 0.0);
        table.insert(1, 20);
        table.insert(2, 30);
        assert_eq!(table.remove(&1), Some(20));
        assert_eq!(table.len(), 1);
        assert_eq!(table.fill_factor(), 0.125);
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
    fn test_rehash() {
        let mut table = HashMap::with_exact_capacity(4, 2, 2, 0.5);
        table.insert(1, "one");
        table.insert(2, "two");
        table.insert(3, "three");
        table.insert(4, "four");
        table.insert(5, "five");
        assert_eq!(table.len(), 5);
        assert_eq!(table.get(&1), Some(&"one"));
        assert_eq!(table.get(&2), Some(&"two"));
        assert_eq!(table.get(&3), Some(&"three"));
        assert_eq!(table.get(&4), Some(&"four"));
        assert_eq!(table.get(&5), Some(&"five"));
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

    #[test]
    fn test_rehash_large() {
        let mut table = HashMap::with_load_factor(0.99);
        table.insert("hello".to_string(), 42);
        assert_eq!(table.buckets.len(), 64);
        for i in 0..1000 {
            table.insert(i.to_string(), i);
        }
        assert!(table.buckets.len() <= 1024 / 4);
    }

    // This fills 10_000 elements into a table with space for exactly 10_000 elements.
    // Access time is constant!
    #[test]
    fn test_fill_factor() {
        let mut table = HashMap::with_exact_capacity(10_000, 4, 4, 1.0);
        for i in 0..10_000 {
            table.insert(i.to_string(), i);
        }
        assert_eq!(table.buckets.len(), 10_000 / 4);
        assert_eq!(table.fill_factor(), 1.0);
    }
}
