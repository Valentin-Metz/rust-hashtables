use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;

use rand::Rng;

pub struct HashMap<K: Hash + Eq, V> {
    buckets: Vec<Option<Entry<K, V>>>,
    length: usize,
    hasher_a: DefaultHasher,
    hasher_b: DefaultHasher,
    load_factor: f64,
}

struct Entry<K: Hash + Eq, V> {
    key: K,
    value: V,
}

impl<K: Hash + Eq, V> HashMap<K, V> {
    pub fn new() -> Self {
        Self::with_exact_capacity(0, 0.4)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_exact_capacity(capacity * 8 * 2, 0.4)
    }
    pub fn with_load_factor(load_factor: f64) -> Self {
        Self::with_exact_capacity(0, load_factor)
    }
    fn with_exact_capacity(capacity: usize, load_factor: f64) -> Self {
        let mut rng = rand::thread_rng();
        let mut hasher_a = DefaultHasher::new();
        let mut hasher_b = DefaultHasher::new();
        hasher_a.write_u64(rng.gen::<u64>());
        hasher_b.write_u64(rng.gen::<u64>());
        Self {
            buckets: (0..capacity).map(|_| None).collect(),
            length: 0,
            hasher_a,
            hasher_b,
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

    fn calculate_hash(key: &K, hasher: &DefaultHasher) -> u64 {
        let mut hasher = hasher.clone();
        key.hash(&mut hasher);
        hasher.finish()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.buckets.is_empty() {
            self.buckets = (0..64).map(|_| None).collect();
        }
        if self.fill_factor() >= self.load_factor {
            self.rehash(2);
        }
        let half = self.buckets.len() / 2;
        let (buckets_a, buckets_b) = self.buckets.split_at_mut(half);
        let entry = Entry { key, value };
        let index_a = Self::calculate_hash(&entry.key, &self.hasher_a) as usize % buckets_a.len();
        let index_b = Self::calculate_hash(&entry.key, &self.hasher_b) as usize % buckets_b.len();
        match (buckets_a.get_mut(index_a), buckets_b.get_mut(index_b)) {
            (Some(bucket_a), Some(bucket_b)) => match (bucket_a, bucket_b) {
                (Some(entry_a), _) if entry_a.key == entry.key => {
                    Some(mem::replace(&mut entry_a.value, entry.value))
                }
                (_, Some(entry_b)) if entry_b.key == entry.key => {
                    Some(mem::replace(&mut entry_b.value, entry.value))
                }
                (bucket_a @ None, _) => {
                    *bucket_a = Some(entry);
                    self.length += 1;
                    None
                }
                (_, bucket_b @ None) => {
                    *bucket_b = Some(entry);
                    self.length += 1;
                    None
                }
                // Kick an entry
                (Some(entry_a), Some(_)) => {
                    let mut entry = mem::replace(entry_a, entry);
                    let mut fill_a = false;
                    for _ in 0..self.length {
                        let index_a = Self::calculate_hash(&entry.key, &self.hasher_a) as usize
                            % buckets_a.len();
                        let index_b = Self::calculate_hash(&entry.key, &self.hasher_b) as usize
                            % buckets_b.len();
                        match (buckets_a.get_mut(index_a), buckets_b.get_mut(index_b)) {
                            (Some(bucket_a), Some(bucket_b)) => match (bucket_a, bucket_b) {
                                (bucket_a @ None, _) => {
                                    *bucket_a = Some(entry);
                                    self.length += 1;
                                    return None;
                                }
                                (_, bucket_b @ None) => {
                                    *bucket_b = Some(entry);
                                    self.length += 1;
                                    return None;
                                }
                                (Some(entry_a), Some(_)) if fill_a => {
                                    entry = mem::replace(entry_a, entry);
                                    fill_a = false;
                                }
                                (Some(_), Some(entry_b)) => {
                                    entry = mem::replace(entry_b, entry);
                                    fill_a = true;
                                }
                            },
                            _ => {
                                unreachable!("index out of bounds");
                            }
                        }
                    }
                    self.rehash(1);
                    self.insert(entry.key, entry.value)
                }
            },
            _ => {
                unreachable!("index out of bounds");
            }
        }
    }

    fn rehash(&mut self, resize_factor: usize) {
        let mut new_table =
            HashMap::with_exact_capacity(self.buckets.len() * resize_factor, self.load_factor);
        for bucket in self.buckets.iter_mut() {
            if let Some(Entry { key, value }) = bucket.take() {
                new_table.insert(key, value);
            }
        }
        mem::swap(self, &mut new_table);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.is_empty() {
            return None;
        }
        let half = self.buckets.len() / 2;
        let (buckets_a, buckets_b) = self.buckets.split_at(half);
        let index_a = Self::calculate_hash(key, &self.hasher_a) as usize % buckets_a.len();
        let index_b = Self::calculate_hash(key, &self.hasher_b) as usize % buckets_b.len();

        match (buckets_a.get(index_a), buckets_b.get(index_b)) {
            (Some(bucket_a), Some(bucket_b)) => match (bucket_a, bucket_b) {
                (Some(entry_a), _) if entry_a.key == *key => Some(&entry_a.value),
                (_, Some(entry_b)) if entry_b.key == *key => Some(&entry_b.value),
                (_, _) => None,
            },
            _ => {
                unreachable!("index out of bounds");
            }
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        if self.is_empty() {
            return None;
        }
        let half = self.buckets.len() / 2;
        let (buckets_a, buckets_b) = self.buckets.split_at_mut(half);
        let index_a = Self::calculate_hash(key, &self.hasher_a) as usize % buckets_a.len();
        let index_b = Self::calculate_hash(key, &self.hasher_b) as usize % buckets_b.len();

        match (buckets_a.get_mut(index_a), buckets_b.get_mut(index_b)) {
            (Some(bucket_a), Some(bucket_b)) => match (bucket_a, bucket_b) {
                (Some(entry_a), _) if entry_a.key == *key => Some(&mut entry_a.value),
                (_, Some(entry_b)) if entry_b.key == *key => Some(&mut entry_b.value),
                (_, _) => None,
            },
            _ => {
                unreachable!("index out of bounds");
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_empty() {
            return None;
        }
        let half = self.buckets.len() / 2;
        let (buckets_a, buckets_b) = self.buckets.split_at_mut(half);
        let index_a = Self::calculate_hash(key, &self.hasher_a) as usize % buckets_a.len();
        let index_b = Self::calculate_hash(key, &self.hasher_b) as usize % buckets_b.len();

        match (buckets_a.get_mut(index_a), buckets_b.get_mut(index_b)) {
            (Some(bucket_a), Some(bucket_b)) => match (&bucket_a, &bucket_b) {
                (Some(entry_a), _) if entry_a.key == *key => {
                    self.length -= 1;
                    Some(bucket_a.take().unwrap().value)
                }
                (_, Some(entry_b)) if entry_b.key == *key => {
                    self.length -= 1;
                    Some(bucket_b.take().unwrap().value)
                }
                (_, _) => None,
            },
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
        let mut table = HashMap::with_exact_capacity(8, 0.2);
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
        let mut table = HashMap::with_exact_capacity(2, 0.2);
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
        let mut table = HashMap::with_exact_capacity(4, 0.2);
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
        let mut table = HashMap::with_capacity(1);
        table.insert("hello".to_string(), 42);
        assert_eq!(table.buckets.len(), 16);
        for i in 0..1000 {
            table.insert(i.to_string(), i);
        }
        assert!(table.buckets.len() >= 2048);
    }

    #[test]
    fn insert_100_000() {
        let mut table = HashMap::with_load_factor(0.50);
        for i in 0..100_000 {
            table.insert(i.to_string(), i);
        }
        assert_eq!(table.len(), 100_000);
    }
}
