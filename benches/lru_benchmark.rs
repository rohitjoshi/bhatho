#[macro_use]
extern crate criterion;

use bhatho::cache::lru_cache::Lru;
use criterion::Criterion;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use std::collections::HashMap;

fn criterion_benchmark(c: &mut Criterion) {
    let capacity = 10_000_000;
    let lru = Lru::new(0, capacity);

    let mut data = HashMap::with_capacity(capacity);

    let mut r_th = rand::thread_rng();
    let mut k = String::new();
    for _ in 0..capacity {
        let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
        let val = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
        cache.put(&key.as_bytes(), &val.as_bytes());
        {
            data.insert(key, val);
        }
    }

    c.bench_function("lru_put", |b| b.iter(|| {
        for (key, val) in data.iter() {
            let mut cache_val = cache.put(key.as_bytes(), val.as_bytes());
        }
    }));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);