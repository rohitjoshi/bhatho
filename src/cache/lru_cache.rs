/************************************************

   File Name: bhatho:cache::lru_cache
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
use lru::LruCache;
use parking_lot::Mutex;
use std::fs::File;
use std::io::Write;
use std::result::Result;
use std::str;
use std::sync::Arc;

//use twox_hash::RandomXxHashBuilder;
//use twox_hash::XxHash;
//use std::collections::HashMap;
use crate::keyval::KeyVal;

//use std::sync::atomic::{Ordering, AtomicUsize};
//type LruCacheVec = HashMap<Vec<u8>, Vec<u8>>;
type LruCacheVec = LruCache<Vec<u8>, Vec<u8>>;

pub struct Lru {
    id: usize,
    cache: Arc<Mutex<LruCacheVec>>,
    cache_capacity: usize,
}

/// send safe
//unsafe impl Send for Lru {}

/// sync safe
//unsafe impl Sync for Lru {}

impl Clone for Lru {
    #[inline]
    fn clone(&self) -> Lru {
        Lru {
            id: self.id,
            cache: self.cache.clone(),
            cache_capacity: self.cache_capacity,
        }
    }
}

impl Lru {
    /// create a new object
    /// make sure path is valid
    pub fn new(id: usize, cache_capacity: usize) -> Lru {
        //let hasher = RandomXxHashBuilder::default();
        //let mut hasher = XxHash::with_seed(0);
        //        let mut cache_capacity = cache_capacity;
        //        match cache_capacity.checked_next_power_of_two() {
        //            Some(power_of_two) => {
        //                cache_capacity = power_of_two
        //            }
        //            None => {}
        //        }
        let cache = Arc::new(Mutex::new(LruCacheVec::new(cache_capacity)));
        //let cache = Arc::new(Mutex::new(HashMap::<Vec<u8>, Vec<u8>>::with_capacity(cache_capacity)));
        Lru {
            id,
            cache,
            cache_capacity,
        }
    }

    /// get key as str
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        //warn!("LruCache::Key:{}, shard:{}, Get",  String::from_utf8_lossy(&key), self.id);
        //get from cache first,
        match self.cache.lock().get(&key.to_vec()) {
            Some(val) => Some(val.to_vec()),
            None => {
                //warn!("LruCache::Key:{}, shard:{}, GetNotFound",  String::from_utf8_lossy(&key), self.id);
                None
            }
        }
    }
    /// get key as str (wrapper function)
    #[inline(always)]
    pub fn get_str(&self, key: &str) -> Option<String> {
        match self.get(key.as_bytes()) {
            Some(val) => Some(String::from_utf8_lossy(&val).to_string()),
            None => None,
        }
    }

    /// put key as str
    #[inline(always)]
    pub fn put_str(&self, key: &str, val: &str) -> Result<(), String> {
        self.put(key.as_bytes(), val.as_bytes())
    }

    /// put key as str
    #[inline(always)]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        //warn!("LruCache::Key:{}, shard:{}, Put",  String::from_utf8_lossy(&key), self.id);
        self.cache.lock().put(key.to_vec(), val.to_vec());
        Ok(())
        /*
        match self.cache.lock().put(String::from_utf8(key.to_vec()).unwrap(), val.to_vec()) {
            Some(_r) => Ok(()), /*returns existing entry*/
        None => Ok(()),  /* new entry inserted successfully */
        }*/
    }

    /// put key as str
    #[inline(always)]
    pub fn batch_put(&self, data: &[KeyVal]) -> Result<(), String> {
        for kv in data.iter() {
            self.cache.lock().put(kv.key.clone(), kv.val.clone());
        }

        Ok(())
    }
    /// delete key
    #[inline(always)]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        self.cache.lock().pop(&key.to_owned());
        //self.cache.lock().pop(&key.to_owned());
        Ok(())
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.cache.lock().len() > 0
    }

    pub fn export_keys(&self, file: &mut File) -> Result<u64, String> {
        let cache = &self.cache.lock();
        debug!("Total Keys {} in shard:{}", cache.len(), self.id);
        let mut total = 0u64;
        for (key, _) in cache.iter() {
            if let Err(e) = file.write(key) {
                error!("export keys: Failed to write to the file.");
                return Err(e.to_string());
            }
            if let Err(e) = file.write(b"\r\n") {
                error!("export keys: Failed to write to the file.");
                return Err(e.to_string());
            }
            total += 1;
        }
        debug!("Total exported keys :{} in shard: {}", total, self.id);
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    //use crate::tests::rand::Rng;
    use super::*;

    #[test]
    fn test_lrucache_put_and_get_large() {
        let capacity = 2000000;
        let mut cache = Lru::new(0, capacity);

        use std::collections::HashMap;
        let mut data = HashMap::with_capacity(capacity);

        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..capacity {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();

            cache.put(&key.as_bytes(), &val.as_bytes());
            {
                data.insert(key, val);
            }
        }

        for (key, val) in data.iter() {
            let mut cache_val = cache.get(key.as_bytes());
            assert!(cache_val.is_some());
            assert_eq!(*val, String::from_utf8_lossy(&cache_val.unwrap()));
        }
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
