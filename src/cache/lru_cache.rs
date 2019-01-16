use std::fs::File;
use std::hash::Hasher;
use std::io::Write;
use std::result::Result;
use std::str;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use twox_hash::RandomXxHashBuilder;

use crate::keyval::KeyVal;

pub struct Lru {
    cache: Arc<Mutex<LruCache<Vec<u8>, Vec<u8>, RandomXxHashBuilder>>>,
    cache_capacity: usize,

}

/// send safe
unsafe impl Send for Lru {}

/// sync safe
unsafe impl Sync for Lru {}

impl Clone for Lru {
    #[inline]
    fn clone(&self) -> Lru {
        Lru {
            cache: self.cache.clone(),
            cache_capacity: self.cache_capacity,

        }
    }
}

impl Lru {
    /// create a new object
    /// make sure path is valid
    pub fn new(cache_capacity: usize) -> Lru {
        let hasher = RandomXxHashBuilder::default();
        let cache = Arc::new(Mutex::new(LruCache::with_hasher(cache_capacity, hasher)));
        Lru {
            cache,
            cache_capacity: cache_capacity,

        }
    }
    /// put key as str
    #[inline]
    pub fn batch_put(&self, data: &[KeyVal]) -> Result<(), String> {
        let mut cache = self.cache.lock().unwrap();
        for kv in data.iter() {
            cache.put(kv.key.clone(), kv.val.clone());
        }

        Ok(())
    }


    /// get key as str
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, String> {
        //get from cache first,
        match self.cache.lock().unwrap().get_mut(&key.to_vec()) {
            Some(val) => {
                return Ok(val.to_vec());
            }
            None => Err(String::from("not found")),
        }
    }
    /// get key as str (wrapper function)
    #[inline]
    pub fn get_str(&self, key: &str) -> Result<String, String> {
        match self.get(key.as_bytes()) {
            Ok(val) => {
                return Ok(String::from_utf8_lossy(&val).to_string());
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// put key as str
    #[inline]
    pub fn put_str(&self, key: &str, val: &str) -> Result<(), String> {
        self.put(key.as_bytes(), val.as_bytes())
    }

    /// put key as str
    #[inline]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        self.cache.lock().unwrap().put(key.to_vec(), val.to_vec());
        Ok(())
        /*
        match self.cache.lock().unwrap().put(String::from_utf8(key.to_vec()).unwrap(), val.to_vec()) {
            Some(_r) => Ok(()), /*returns existing entry*/
        None => Ok(()),  /* new entry inserted successfully */
        }*/
    }
    /// delete key
    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        self.cache.lock().unwrap().pop(&key.to_vec());
        //self.cache.lock().unwrap().remove(&key.to_owned());
        Ok(())
    }

    pub fn export_keys(&self, file: &mut File) -> Result<(), String> {
        let cache = &self.cache.lock().unwrap();

        for (key, _) in cache.iter() {
            file.write(key);
            file.write(b"\r\n");
        }
        Ok(())
    }
}
/*
#[cfg(test)]
mod tests {
    extern crate test;

    use std::collections::HashMap;
    use std::hash::Hasher;
    use std::thread;

    use super::*;

    use self::rand::distributions::Alphanumeric;
    use self::rand::prelude::*;
    use self::rand::Rng;
    use self::test::Bencher;

    extern crate rand;

    #[bench]
    fn Lru_insert_to_cache_bench(b: &mut Bencher) {
        let total = 5_000;

        let mut data: HashMap<String, String> = HashMap::with_capacity(total);
        let mut cache = LruCache::new(total);
        let mut r_th = rand::thread_rng();

        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(256)
                .collect::<String>();
            data.insert(key, val);
        }
        b.iter(|| {
            for (key, val) in data.iter() {
                let mut val = cache.put(key, val);
            }
        });
        let x = data.len();
    }

    #[bench]
    fn Lru_xxhash_insert_to_cache_bench(b: &mut Bencher) {
        let total = 5_000;

        let mut data: HashMap<String, String> = HashMap::with_capacity(total);
        let hasher = RandomXxHashBuilder::default();
        let mut cache = LruCache::with_hasher(total, hasher);
        //let mut cache = LruCache::new(total);
        let mut r_th = rand::thread_rng();

        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(256)
                .collect::<String>();
            data.insert(key, val);
        }
        b.iter(|| {
            for (key, val) in data.iter() {
                let mut val = cache.put(key, val);
            }
        });
        let x = data.len();
    }

    #[bench]
    fn Lru_get_from_cache_bench(b: &mut Bencher) {
        let total = 5_000;
        //let mut cache = Lru::new(total);
        let mut cache = LruCache::new(total);
        let mut data: HashMap<String, String> = HashMap::with_capacity(total);

        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(256)
                .collect::<String>();
            cache.put(key.clone(), val.clone());
            //k = key.clone();
            data.insert(key, val);
        }

        b.iter(|| {
            for (key, _) in data.iter() {
                let mut val = cache.get(&key);
            }
        });
    }

    #[bench]
    fn Lru_xxhash_get_from_cache_bench(b: &mut Bencher) {
        let total = 5_000;
        //let mut cache = Lru::new(total);
        let hasher = RandomXxHashBuilder::default();
        let mut cache = LruCache::with_hasher(total, hasher);
        let mut data: HashMap<String, String> = HashMap::with_capacity(total);

        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(256)
                .collect::<String>();
            cache.put(key.clone(), val.clone());
            //k = key.clone();
            data.insert(key, val);
        }

        b.iter(|| {
            for (key, _) in data.iter() {
                let mut val = cache.get(&key);
            }
        });
    }

    #[bench]
    fn Lru_get_from_cache_notfound_bench(b: &mut Bencher) {
        let total = 5_000;
        let mut cache = Lru::new(total);
        let mut data: HashMap<String, String> = HashMap::with_capacity(total);
        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(256)
                .collect::<String>();
            // cache.put_str(&key, &val);
            k = key.clone();
            data.insert(key, val);
        }

        b.iter(|| {
            for (key, _) in data.iter() {
                let mut val = cache.get_str(&key);
            }
        });
    }

    #[bench]
    fn Lru_get_from_cache_bench_mt(b: &mut Bencher) {
        let total = 10_000;
        let mut cache = Lru::new(total);
        let mut data: HashMap<String, String> = HashMap::with_capacity(total);
        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(256)
                .collect::<String>();
            cache.put_str(&key, &val);
            // k = key.clone();
            data.insert(key, val);
        }
        let ca = Arc::new(cache);
        b.iter(|| {
            let writers: Vec<_> = (0..8)
                .map(|i| {
                    let d = data.clone();
                    let e = ca.clone();

                    thread::spawn(move || {
                        for (key, _) in d.iter() {
                            let mut val = e.get_str(&key);
                        }
                        for (key, val) in d.iter() {
                            let mut val = e.put_str(&key, &val);
                        }
                    })
                })
                .collect();
        });
    }

    #[bench]
    fn get_shard1_bench(b: &mut Bencher) {
        let mut r_th = rand::thread_rng();
        let data = r_th.sample_iter(&Alphanumeric)
            .take(128)
            .collect::<String>()
            .into_bytes();
        let num_shards: u16 = 128;
        let mut x: usize = 0;
        b.iter(|| {
            for _ in 0..100000 {
                x = ShardedCache::get_shard1(&data, num_shards);
            }
        });
    }

    #[bench]
    fn get_shard2_bench(b: &mut Bencher) {
        let mut r_th = rand::thread_rng();
        let data = r_th.sample_iter(&Alphanumeric)
            .take(128)
            .collect::<String>()
            .into_bytes();
        let num_shards: u16 = 128;
        let mut x: usize = 0;
        b.iter(|| {
            for _ in 0..100000 {
                x = ShardedCache::get_shard2(&data, num_shards);
            }
        });
    }
}*/
