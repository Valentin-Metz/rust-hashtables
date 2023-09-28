use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::collections;
use std::sync::Arc;

use dashmap::DashMap;
use hashtables::chaining_hashing::HashMap as ChainingHashMap;
use hashtables::chaining_hashing_concurrent::HashMap as ConcurrentChainingHashMap;
use hashtables::chaining_hashing_concurrent_optimized::HashMap as ConcurrentChainingHashMapOptimized;
use hashtables::cuckoo_hashing::HashMap as CuckooHashMap;
use hashtables::open_hashing::HashMap as OpenHashMap;
use hashtables::quad_cuckoo_hashing::HashMap as QuadCuckooHashMap;
use parking_lot::Mutex;

// ChainingHashMap
pub fn insert_chaining(c: &mut Criterion) {
    let mut group = c.benchmark_group("ChainingHashMap insert");
    for load_factor in [
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0,
    ]
    .iter()
    {
        group.bench_with_input(
            format!("load_factor={:05.2}", load_factor),
            load_factor,
            |b, &load_factor| {
                b.iter(|| {
                    let mut table = ChainingHashMap::with_load_factor(load_factor);
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.insert(i, i), None);
                    }
                })
            },
        );
    }
}
// CuckooHashMap
pub fn insert_cuckoo(c: &mut Criterion) {
    let mut group = c.benchmark_group("CuckooHashMap insert");
    for load_factor in [0.1, 0.2, 0.3, 0.4, 0.45, 0.5, 0.51, 0.52, 0.53].iter() {
        group.bench_with_input(
            format!("load_factor={:.2}", load_factor),
            load_factor,
            |b, &load_factor| {
                b.iter(|| {
                    let mut table = CuckooHashMap::with_load_factor(load_factor);
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.insert(i, i), None);
                    }
                })
            },
        );
    }
}
// OpenHashMap
pub fn insert_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("OpenHashMap insert");
    for load_factor in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0].iter() {
        group.bench_with_input(
            format!("load_factor={:.2}", load_factor),
            load_factor,
            |b, &load_factor| {
                b.iter(|| {
                    let mut table = OpenHashMap::with_load_factor(load_factor);
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.insert(i, i), None);
                    }
                })
            },
        );
    }
}
// QuadCuckooHashMap
pub fn insert_quad_cuckoo(c: &mut Criterion) {
    let mut group = c.benchmark_group("QuadCuckooHashMap insert");
    for load_factor in [
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.97, 0.98, 0.99, 0.995, 0.999,
    ]
    .iter()
    {
        group.bench_with_input(
            format!("load_factor={:.3}", load_factor),
            load_factor,
            |b, &load_factor| {
                b.iter(|| {
                    let mut table = QuadCuckooHashMap::with_load_factor(load_factor);
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.insert(i, i), None);
                    }
                })
            },
        );
    }
}

pub fn compare_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashMap compared insert");
    // ChainingHashMap
    group.bench_function("ChainingHashMap", |b| {
        b.iter(|| {
            let mut table = ChainingHashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
    // OpenHashMap
    group.bench_function("OpenHashMap", |b| {
        b.iter(|| {
            let mut table = OpenHashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
    // CuckooHashMap
    group.bench_function("CuckooHashMap", |b| {
        b.iter(|| {
            let mut table = CuckooHashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
    // QuadCuckooHashMap
    group.bench_function("QuadCuckooHashMap", |b| {
        b.iter(|| {
            let mut table = QuadCuckooHashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
    // std::collections::HashMap
    group.bench_function("std::collections::HashMap", |b| {
        b.iter(|| {
            let mut table = std::collections::HashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
}

// ChainingHashMap
pub fn get_chaining(c: &mut Criterion) {
    let mut group = c.benchmark_group("ChainingHashMap get");
    for load_factor in [
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0,
    ]
    .iter()
    {
        // CuckooHashMap
        group.bench_with_input(
            format!("load_factor={:05.2}", load_factor),
            load_factor,
            |b, &load_factor| {
                let mut table = ChainingHashMap::with_load_factor(load_factor);
                for i in 0..100_000 {
                    assert_eq!(table.insert(i, i), None);
                }
                b.iter(|| {
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.get(&i), Some(&i));
                    }
                })
            },
        );
    }
}
// CuckooHashMap
pub fn get_cuckoo(c: &mut Criterion) {
    let mut group = c.benchmark_group("CuckooHashMap get");
    for load_factor in [0.1, 0.2, 0.3, 0.4, 0.45, 0.5, 0.51, 0.52, 0.53].iter() {
        // CuckooHashMap
        group.bench_with_input(
            format!("load_factor={:.2}", load_factor),
            load_factor,
            |b, &load_factor| {
                let mut table = CuckooHashMap::with_load_factor(load_factor);
                for i in 0..100_000 {
                    assert_eq!(table.insert(i, i), None);
                }
                b.iter(|| {
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.get(&i), Some(&i));
                    }
                })
            },
        );
    }
}
// OpenHashMap
pub fn get_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("OpenHashMap get");
    for load_factor in [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0].iter() {
        // CuckooHashMap
        group.bench_with_input(
            format!("load_factor={:.2}", load_factor),
            load_factor,
            |b, &load_factor| {
                let mut table = OpenHashMap::with_load_factor(load_factor);
                for i in 0..100_000 {
                    assert_eq!(table.insert(i, i), None);
                }
                b.iter(|| {
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.get(&i), Some(&i));
                    }
                })
            },
        );
    }
}
// QuadCuckooHashMap
pub fn get_quad_cuckoo(c: &mut Criterion) {
    let mut group = c.benchmark_group("QuadCuckooHashMap get");
    for load_factor in [
        0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.97, 0.98, 0.99, 0.995, 0.999,
    ]
    .iter()
    {
        // CuckooHashMap
        group.bench_with_input(
            format!("load_factor={:.3}", load_factor),
            load_factor,
            |b, &load_factor| {
                let mut table = QuadCuckooHashMap::with_load_factor(load_factor);
                for i in 0..100_000 {
                    assert_eq!(table.insert(i, i), None);
                }
                b.iter(|| {
                    let n = black_box(100_000);
                    for i in 0..n {
                        assert_eq!(table.get(&i), Some(&i));
                    }
                })
            },
        );
    }
}

pub fn compare_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashMap compared get");
    // ChainingHashMap
    group.bench_function("ChainingHashMap", |b| {
        let mut table = ChainingHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
    // CuckooHashMap
    group.bench_function("CuckooHashMap", |b| {
        let mut table = CuckooHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
    // OpenHashMap
    group.bench_function("OpenHashMap", |b| {
        let mut table = OpenHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
    // QuadCuckooHashMap
    group.bench_function("QuadCuckooHashMap", |b| {
        let mut table = QuadCuckooHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
    // std::collections::HashMap
    group.bench_function("std::collections::HashMap", |b| {
        let mut table = std::collections::HashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
}

pub fn concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashMap concurrent insert");
    // std::collections::HashMap
    group.bench_function("std::collections::HashMap (single-threaded)", |b| {
        b.iter(|| {
            let mut table = collections::HashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
    // std::collections::HashMap Mutex
    group.bench_function(
        "std::collections::HashMap (multi-threaded) with global Mutex",
        |b| {
            b.iter(|| {
                let table = Mutex::new(collections::HashMap::new());
                let n = black_box(100_000);
                (0..n).into_par_iter().for_each(|i| {
                    assert_eq!(table.lock().insert(i, i), None);
                });
            })
        },
    );
    // ChainingHashMap
    group.bench_function("ChainingHashMap (single-threaded)", |b| {
        b.iter(|| {
            let mut table = ChainingHashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, i), None);
            }
        })
    });
    // ChainingHashMap Mutex
    group.bench_function("ChainingHashMap (multi-threaded) with global Mutex", |b| {
        b.iter(|| {
            let table = Mutex::new(ChainingHashMap::new());
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.lock().insert(i, i), None);
            });
        })
    });
    // ConcurrentChainingHashMap single-threaded
    group.bench_function("ConcurrentChainingHashMap (single-threaded)", |b| {
        b.iter(|| {
            let table = ConcurrentChainingHashMap::new();
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.insert(i, Arc::new(i)), None);
            }
        })
    });
    // ConcurrentChainingHashMap multi-threaded
    group.bench_function("ConcurrentChainingHashMap (multi-threaded)", |b| {
        b.iter(|| {
            let table = ConcurrentChainingHashMap::new();
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.insert(i, Arc::new(i)), None);
            });
        })
    });
    // ConcurrentChainingHashMapOptimized single-threaded
    group.bench_function(
        "ConcurrentChainingHashMapOptimized (single-threaded)",
        |b| {
            b.iter(|| {
                let table = ConcurrentChainingHashMapOptimized::with_capacity(100_000);
                let n = black_box(100_000);
                for i in 0..n {
                    assert_eq!(table.insert(i, Arc::new(i)), None);
                }
            })
        },
    );
    // ConcurrentChainingHashMapOptimized multi-threaded
    group.bench_function("ConcurrentChainingHashMapOptimized (multi-threaded)", |b| {
        b.iter(|| {
            let table = ConcurrentChainingHashMapOptimized::with_capacity(100_000);
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.insert(i, Arc::new(i)), None);
            });
        })
    });
    // DashMap multi-threaded
    group.bench_function("DashMap (multi-threaded)", |b| {
        b.iter(|| {
            let table = DashMap::new();
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.insert(i, Arc::new(i)), None);
            });
        })
    });
}

pub fn concurrent_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("HashMap concurrent get");
    // std::collections::HashMap
    group.bench_function("std::collections::HashMap (single-threaded)", |b| {
        let mut table = collections::HashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
    // std::collections::HashMap multi-threaded
    group.bench_function("std::collections::HashMap (multi-threaded)", |b| {
        let mut table = ChainingHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.get(&i), Some(&i));
            });
        })
    });
    // ChainingHashMap
    group.bench_function("ChainingHashMap (single-threaded)", |b| {
        let mut table = ChainingHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(&i));
            }
        })
    });
    // ChainingHashMap multi-threaded
    group.bench_function("ChainingHashMap (multi-threaded)", |b| {
        let mut table = ChainingHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, i), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.get(&i), Some(&i));
            });
        })
    });
    // ConcurrentChainingHashMap single-threaded
    group.bench_function("ConcurrentChainingHashMap (single-threaded)", |b| {
        let table = ConcurrentChainingHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, Arc::new(i)), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            for i in 0..n {
                assert_eq!(table.get(&i), Some(Arc::new(i)));
            }
        })
    });
    // ConcurrentChainingHashMap multi-threaded
    group.bench_function("ConcurrentChainingHashMap (multi-threaded)", |b| {
        let table = ConcurrentChainingHashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, Arc::new(i)), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.get(&i), Some(Arc::new(i)));
            });
        })
    });
    // ConcurrentChainingHashMapOptimized single-threaded
    group.bench_function(
        "ConcurrentChainingHashMapOptimized (single-threaded)",
        |b| {
            let table = ConcurrentChainingHashMapOptimized::with_capacity(100_000);
            for i in 0..100_000 {
                assert_eq!(table.insert(i, Arc::new(i)), None);
            }
            b.iter(|| {
                let n = black_box(100_000);
                for i in 0..n {
                    assert_eq!(table.get(&i), Some(Arc::new(i)));
                }
            })
        },
    );
    // ConcurrentChainingHashMapOptimized multi-threaded
    group.bench_function("ConcurrentChainingHashMapOptimized (multi-threaded)", |b| {
        let table = ConcurrentChainingHashMapOptimized::with_capacity(100_000);
        for i in 0..100_000 {
            assert_eq!(table.insert(i, Arc::new(i)), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(table.get(&i), Some(Arc::new(i)));
            });
        })
    });
    // DashMap multi-threaded
    group.bench_function("DashMap (multi-threaded)", |b| {
        let table = DashMap::new();
        for i in 0..100_000 {
            assert_eq!(table.insert(i, Arc::new(i)), None);
        }
        b.iter(|| {
            let n = black_box(100_000);
            (0..n).into_par_iter().for_each(|i| {
                assert_eq!(*(table.get(&i).unwrap()), Arc::new(i));
            });
        })
    });
}

criterion_group!(
    hash_table,
    insert_chaining,
    insert_cuckoo,
    insert_open,
    insert_quad_cuckoo,
    get_chaining,
    get_cuckoo,
    get_open,
    get_quad_cuckoo,
    compare_insert,
    compare_get,
    concurrent_insert,
    concurrent_get,
);
criterion_main!(hash_table);
