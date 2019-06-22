/************************************************

   File Name: bhatho:cache::sharded_cache
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/

use std::fs::OpenOptions;
use std::sync::Arc;

use crate::cache::config::CacheConfig;
use crate::cache::lru_cache::Lru;
use crate::keyval::KeyVal;
use std::fs;
use std::path::Path;

pub struct ShardedCache {
    pub shards: Arc<Vec<Lru>>,
    config: CacheConfig,
    enabled: bool,
}

//unsafe impl Send for ShardedCache {}

//unsafe impl Sync for ShardedCache {}

impl Clone for ShardedCache {
    fn clone(&self) -> ShardedCache {
        ShardedCache {
            shards: self.shards.clone(),
            config: self.config.clone(),
            enabled: self.enabled,
        }
    }
}

impl ShardedCache {
    ///
    /// get shard logic is simple. mod of hash code with number of db instances.
    /// in future, we can improve by different criteria
    /// e.g Key suffix or prefix
    #[inline(always)]
    fn get_shard(&self, key: &[u8]) -> usize {
        KeyVal::key_slot(key, self.config.num_shards) as usize
    }

    #[inline(always)]
    fn get_shard_key_val(&self, kv: &KeyVal) -> usize {
        kv.slot(self.config.num_shards) as usize
    }

    #[inline(always)]
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// create a new object
    /// make sure path is valid
    pub fn new(config: &CacheConfig) -> ShardedCache {
        assert!(config.num_shards > 0);
        let adjust = config.cache_capacity % config.num_shards as usize;
        let shard_capacity = (config.cache_capacity + adjust) / config.num_shards as usize;

        assert!(shard_capacity > 0);
        let mut shards: Vec<Lru> = Vec::with_capacity(config.num_shards as usize);
        if config.enabled {
            info!(
                "cache_capacity:{}, num_shards:{}, shard_capacity: {}",
                config.cache_capacity, config.num_shards, shard_capacity
            );
            for i in 0..config.num_shards {
                let lru = Lru::new(i, shard_capacity);
                shards.push(lru);
            }
        } else {
            warn!("LruCache not enabled");
        }

        ShardedCache {
            shards: Arc::new(shards),
            config: config.clone(),
            enabled: config.enabled,
        }
    }

    #[inline]
    pub fn get_lru_shard(&self, shard: usize) -> &Lru {
        //let shard = self.get_shard(&key);
        &self.shards[shard]
    }
    /// put key as str
    #[inline]
    pub fn batch_put(&self, data: &[KeyVal]) -> Result<(), String> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return Ok(());
        }
        for kv in data.iter() {
            let shard = self.get_shard_key_val(&kv);
            if let Err(e) = self.shards[shard].put(&kv.key, &kv.val) {
                debug!(
                    "Insert failed for the key: {}. Error: {:?}",
                    String::from_utf8_lossy(&kv.key),
                    e
                );
            }
        }
        Ok(())
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return None;
        }
        let shard = self.get_shard(&key);
        self.shards[shard].get(&key)
    }

    #[inline]
    pub fn get_key_val(&self, kv: &KeyVal) -> Option<Vec<u8>> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return None;
        }
        let shard = self.get_shard_key_val(&kv);
        self.shards[shard].get(&kv.key)
    }

    #[inline]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return Ok(());
        }
        let shard = self.get_shard(&key);
        self.shards[shard].put(&key, &val)
    }

    #[inline]
    pub fn put_key_val(&self, kv: &KeyVal, val: &[u8]) -> Result<(), String> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return Ok(());
        }
        let shard = self.get_shard_key_val(&kv);
        self.shards[shard].put(&kv.key, &val)
    }

    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return Ok(());
        }
        let shard = self.get_shard(&key);
        self.shards[shard].delete(&key)
    }

    pub fn export_keys(&self) -> Result<u64, String> {
        if !self.enabled {
            debug!("Cache is not enabled");
            return Ok(0);
        }

        if !self.config.keys_dump_enabled {
            info!(
                "Exporting lru keys not enabled for export key file :{}",
                self.config.keys_dump_file
            );
            return Ok(0);
        }

        warn!("This is a blocking operation");
        info!("Exporting keys from the cache");

        let path = Path::new(&self.config.keys_dump_file);
        //if path doesn't exist, create the directory
        if !path.exists() {
            debug!(
                "Path: {:?} doesn't exist. Creating parent directories",
                path
            );
            if let Some(parent) = path.parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    error!(
                        "Failed to create a directory: {:?} for exporting keys. Error: {:?}",
                        parent, e
                    );
                    return Err(e.to_string());
                } else {
                    debug!("Successfully created directory path: {:?}", parent);
                }
            }
        }
        let mut file = match OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.config.keys_dump_file.as_str())
        {
            Err(e) => {
                error!(
                    "Failed to open file: {} for exporting keys. Error:{:?}",
                    self.config.keys_dump_file, e
                );
                return Err(e.to_string());
            }
            Ok(f) => {
                info!(
                    "Successfully opened file: {} for exporting keys",
                    self.config.keys_dump_file
                );
                f
            }
        };
        let mut total = 0u64;
        for i in 0..self.shards.len() {
            let count = self.shards[i].export_keys(&mut file)?;
            info!(
                "LRU Shard:{} Exported {} keys to file {}",
                i, count, self.config.keys_dump_file
            );

            total += count;
        }
        if let Err(e) = file.sync_data() {
            error!("Failed to execute file sync_data(). Error: {:?}", e);
        }
        info!(
            "Successfully exported {} keys from the cache to file {}",
            total, self.config.keys_dump_file
        );
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    //use crate::tests::rand::Rng;
    extern crate scoped_threadpool;

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use scoped_threadpool::Pool;
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_sharded_cache_put_and_get_large() {
        let capacity = 2000000;
        let mut cache = Lru::new(0, capacity);

        let mut data = HashMap::with_capacity(capacity);

        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..capacity {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = format!("{}", (KeyVal::get_hash_code(&key.as_bytes()) as usize));

            cache.put(&key.as_bytes(), &val.as_bytes());
            data.insert(key, val);
        }

        for (key, val) in data.iter() {
            let mut cache_val = cache.get(key.as_bytes());
            assert!(cache_val.is_some());
            assert_eq!(*val, String::from_utf8_lossy(&cache_val.unwrap()));
        }
    }

    #[test]
    fn test_sharded_cache_put_and_get_multi_shards() {
        let num_shards = 32;
        let capacity = 2000000;
        let shard_capacity = capacity / num_shards;
        let mut shards = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let mut cache = Lru::new(i, capacity);
            shards.push(cache)
        }

        let mut data = HashMap::with_capacity(capacity);

        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..capacity {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
            let s = hash % num_shards;
            let val = format!("{}", hash);

            shards[s].put(&key.as_bytes(), &val.as_bytes());
            data.insert(key, val);
        }

        for (key, val) in data.iter() {
            let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
            let s = hash % num_shards;
            let mut cache_val = shards[s].get(key.as_bytes());
            assert!(cache_val.is_some());
            assert_eq!(*val, String::from_utf8_lossy(&cache_val.unwrap()));
        }
    }

    #[test]
    fn test_sharded_lru_cache_put_and_get_multi_shards_mt() {
        let num_shards = 32;
        let capacity = 1_000_000;
        let shard_capacity = capacity / num_shards;
        let mut shards = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let mut cache = Lru::new(i, capacity);
            shards.push(cache);
        }
        let mut shards = Arc::new(shards);
        let mut data = HashMap::with_capacity(capacity);
        let mut pool = Pool::new(8);
        let shards_ref = &shards;
        pool.scoped(|scoped| {
            for _ in 0..capacity {
                let mut r_th = rand::thread_rng();
                let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
                let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
                let s = hash % num_shards;
                let val = format!("{}", hash);
                data.insert(key.clone(), val.clone());
                scoped.execute(move || {
                    shards_ref[s].put(&key.as_bytes(), &val.as_bytes());
                });
            }
        });
        println!("Insert completed");
        let shards_ref = &shards;
        pool.scoped(|scoped| {
            for (key, val) in data.iter() {
                scoped.execute(move || {
                    let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
                    let s = hash % num_shards;
                    let mut cache_val = shards_ref[s].get(key.as_bytes());
                    assert!(cache_val.is_some());
                    assert_eq!(*val, String::from_utf8_lossy(&cache_val.unwrap()));
                });
            }
        });
    }

    #[test]
    fn test_sharded_cache_put_and_get_multi_shards_mt() {
        let num_shards = 1024;
        let capacity = 10_000_000;
        let mut cache_config = CacheConfig::default();
        cache_config.cache_capacity = capacity;
        cache_config.num_shards = num_shards;
        for i in 6..32 {
            println!("Test started for key length: {}", i);
            let shared_cache = ShardedCache::new(&cache_config);
            let mut data = HashMap::with_capacity(capacity);
            let mut pool = Pool::new(8);
            let shards_ref = &shared_cache;
            pool.scoped(|scoped| {
                for _ in 0..capacity {
                    let mut r_th = rand::thread_rng();
                    let key = r_th.sample_iter(&Alphanumeric).take(i).collect::<String>();
                    let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
                    let val = format!("{}", hash);
                    data.insert(key.clone(), val.clone());
                    scoped.execute(move || {
                        shards_ref.put(&key.as_bytes(), &val.as_bytes());
                    });
                }
            });
            ;
            println!("Insert completed for key length:{}", i);
            let shards_ref = &shared_cache;
            pool.scoped(|scoped| {
                for (key, val) in data.iter() {
                    scoped.execute(move || {
                        let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
                        let mut cache_val = shards_ref.get(key.as_bytes());
                        assert!(cache_val.is_some());

                        assert_eq!(*val, String::from_utf8_lossy(&cache_val.unwrap()));
                    });
                }
            });
            println!("Test completed for key length: {}", i);
        }
    }

    #[test]
    fn test_sharded_cache_custom_put_and_get_multi_shards_mt() {
        struct ShardedTestCache {
            pub shards: Arc<Vec<Lru>>,
            pub num_shards: usize,
        }
        impl ShardedTestCache {
            fn get_shard(&self, key: &[u8]) -> usize {
                //let jh = jumphash::JumpHasher::new();
                //jh.slot(&key, self.config.num_shards as u32) as usize
                (KeyVal::get_hash_code(&key) % self.num_shards as u64) as usize
            }
            pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
                let shard = self.get_shard(&key);
                self.shards[shard].get(&key)
            }
            pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
                let shard = self.get_shard(&key);
                self.shards[shard].put(&key, &val)
            }

            pub fn new(num_shards: usize, capacity: usize) -> ShardedTestCache {
                let shard_capacity = capacity / num_shards;
                let mut shards = Vec::with_capacity(num_shards);

                for i in 0..num_shards {
                    let mut cache = Lru::new(i, shard_capacity);
                    shards.push(cache);
                }
                ShardedTestCache {
                    num_shards,
                    shards: Arc::new(shards),
                }
            }
        }

        let num_shards = 32;
        let capacity = 1_000_000;
        let mut cache_config = CacheConfig::default();
        cache_config.cache_capacity = capacity;
        cache_config.num_shards = num_shards;
        let shared_cache = ShardedTestCache::new(num_shards, capacity);
        let mut data = HashMap::with_capacity(capacity);
        let mut pool = Pool::new(8);
        let shards_ref = &shared_cache;
        pool.scoped(|scoped| {
            for _ in 0..capacity {
                let mut r_th = rand::thread_rng();
                let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
                let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
                let val = format!("{}", hash);
                data.insert(key.clone(), val.clone());
                scoped.execute(move || {
                    shards_ref.put(&key.as_bytes(), &val.as_bytes());
                });
            }
        });
        ;
        println!("Insert completed");
        let shards_ref = &shared_cache;
        pool.scoped(|scoped| {
            for (key, val) in data.iter() {
                scoped.execute(move || {
                    let hash = (KeyVal::get_hash_code(&key.as_bytes()) as usize);
                    let s = hash % num_shards;
                    let mut cache_val = shards_ref.get(key.as_bytes());
                    assert!(cache_val.is_some());
                    assert_eq!(*val, String::from_utf8_lossy(&cache_val.unwrap()));
                });
            }
        });
    }

    #[test]
    fn test_sharded_cache_keyval_hashcode() {
        let capacity = 2000000;
        let mut cache = HashMap::with_capacity(capacity);
        let mut data = HashMap::with_capacity(capacity);

        let mut r_th = rand::thread_rng();
        let mut k = "test".to_string();
        for _ in 0..capacity {
            let key = r_th.sample_iter(&Alphanumeric).take(8).collect::<String>();
            let val = format!("{}", (KeyVal::get_hash_code(&key.as_bytes()) as usize));

            cache.insert(key.clone(), val.clone());
            data.insert(key, val);
        }

        for (key, val) in data.iter() {
            let mut cache_val = cache.get(key);
            assert!(cache_val.is_some());
            assert_eq!(val, cache_val.unwrap());
        }
    }
}
