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

pub struct ShardedCache {
    pub shards: Arc<Vec<Lru>>,
    config: CacheConfig,
}

unsafe impl Send for ShardedCache {}

unsafe impl Sync for ShardedCache {}

impl Clone for ShardedCache {
    fn clone(&self) -> ShardedCache {
        ShardedCache {
            shards: self.shards.clone(),
            config: self.config.clone(),

        }
    }
}

impl ShardedCache {
    /*#[inline]
    pub fn get_shard(&self, data: &[u8]) -> usize {
        if self.num_shards < 2 {
            return 0;
        }
        ShardedCache::get_shard2(&data, self.num_shards)
    }
    #[inline]
    pub fn get_shard1(data: &[u8], num_shards: u16) -> usize {
        let mut total: u64 = 0;
        for d in data.iter() {
            total += *d as u64;
        }
        ((total % num_shards as u64) as usize)
    }
    #[inline]
    pub fn get_shard2(data: &[u8], num_shards: u16) -> usize {
        let mut hasher = XxHash::with_seed(0);
        hasher.write(&data);
        let total: u64 = hasher.finish();
        ((total % num_shards as u64) as usize)
    }*/


    ///
    /// get shard logic is simple. mod of hash code with number of db instances.
    /// in future, we can improve by different criteria
    /// e.g Key suffix or prefix
    fn get_shard(&self, key: &[u8]) -> usize {
        KeyVal::get_hash_code(&key) as usize % self.config.num_shards
    }

    fn get_shard_key_val(&self, key_val: &KeyVal) -> usize {
        key_val.hash as usize % self.config.num_shards
    }

    #[inline]
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// create a new object
    /// make sure path is valid
    pub fn new(config: &CacheConfig) -> ShardedCache {
        assert!(config.num_shards > 0);
        let shard_capacity = config.cache_capacity / config.num_shards as usize;
        assert!(shard_capacity > 0);
        let mut shards: Vec<Lru> = Vec::with_capacity(config.num_shards as usize);
        if config.enabled {
            for _ in 0..config.num_shards {
                shards.push(Lru::new(shard_capacity));
            }
        }


        ShardedCache {
            shards: Arc::new(shards),
            config: config.clone(),
        }
    }
    /// put key as str
    #[inline]
    pub fn batch_put(&self, data: &[KeyVal]) -> Result<(), String> {
        if !self.config.enabled {
            return Err(String::from("Cache is not enabled"));
        }
        for kv in data.iter() {
            if let Err(e) = self.shards[self.get_shard_key_val(&kv)].put(&kv.key, &kv.val) {
                debug!("Insert failed for the key: {}. Error: {:?}", String::from_utf8_lossy(&kv.key), e);
            }
        }
        Ok(())
    }
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, String> {
        if !self.config.enabled {
            return Err(String::from("Cache is not enabled"));
        }
        self.shards[self.get_shard(&key)].get(&key)
    }

    #[inline]
    pub fn get_key_val(&self, kv: &KeyVal) -> Result<Vec<u8>, String> {
        if !self.config.enabled {
            return Err(String::from("Cache is not enabled"));
        }
        self.shards[self.get_shard_key_val(&kv)].get(&kv.key)
    }

    #[inline]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        if !self.config.enabled {
            return Err(String::from("Cache is not enabled"));
        }
        self.shards[self.get_shard(&key)].put(&key, &val)
    }

    #[inline]
    pub fn put_key_val(&self, kv: &KeyVal) -> Result<(), String> {
        if !self.config.enabled {
            return Err(String::from("Cache is not enabled"));
        }
        self.shards[self.get_shard_key_val(&kv)].put(&kv.key, &kv.val)
    }

    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        if !self.config.enabled {
            return Err(String::from("Cache is not enabled"));
        }
        self.shards[self.get_shard(&key)].delete(&key)
    }

    pub fn export_keys(&self) -> Result<u64, String> {
        warn!("This is a blocking operation");
        info!("Exporting keys from the cache");
        let mut file = match OpenOptions::new().write(true)
            .create(true).open(self.config.keys_dump_file.as_str()) {
            Err(e) => {
                error!("Failed to open file :{} for exporting keys. Error:{:?}", self.config.keys_dump_file, e);
                return Err(e.to_string());
            }
            Ok(f) => {
                info!("Successfully opened file: {} for exporting keys", self.config.keys_dump_file);
                f
            }
        };
        let mut total = 0u64;
        for i in 0..self.shards.len() {
            let count = self.shards[i].export_keys(&mut file)?;

            total += count;
        }
        if let Err(e) = file.sync_data() {
            error!("Failed to execute file sync_data(). Error: {:?}", e);
        }
        info!("Successfully exported {} keys from the cache to file {}", total, self.config.keys_dump_file);
        Ok(total)
    }
}