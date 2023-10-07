# rust_hashtables

This repository contains and compares multiple different hash table implementations in Rust.

Including a
[cuckoo-hash table with an arbitrary amount of hash functions and arbitrary bucket size](https://github.com/Valentin-Metz/rust_hashtables/blob/main/src/quad_cuckoo_hashing.rs);
operating with constant insert and access time at 99.99% fill-factor.
Modeled
after [this](https://citeseerx.ist.psu.edu/doc/10.1.1.87.8997)
microsoft research paper.

For benchmark results, see [here](https://valentin-metz.github.io/rust_hashtables/report/index.html).

Most notably, the comparing benchmarks:

- [get](https://valentin-metz.github.io/rust_hashtables/HashMap%20compared%20get/report/index.html)
- [insert](https://valentin-metz.github.io/rust_hashtables/HashMap%20compared%20insert/report/index.html)
- [concurrent get](https://valentin-metz.github.io/rust_hashtables/HashMap%20concurrent%20get/report/index.html)
- [concurrent insert](https://valentin-metz.github.io/rust_hashtables/HashMap%20concurrent%20insert/report/index.html)
