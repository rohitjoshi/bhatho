/************************************************

   File Name: bhatho:db::db_manager
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::cache::sharded_cache::ShardedCache;
use crate::db::config::DbManagerConfig;
use crate::db::rocks_db::RocksDb;
use crate::keyval::KeyVal;

/// DbManager
/// It is a wrapper around multiple database instances
pub struct DbManager {
    pub enabled: bool,
    pub name: String,
    db: Arc<RocksDb>,
    cache: Arc<ShardedCache>,
    config: DbManagerConfig,
}

/// Clone the instance
impl Clone for DbManager {
    #[inline]
    fn clone(&self) -> DbManager {
        DbManager {
            enabled: self.enabled,
            name: self.name.clone(),
            db: self.db.clone(),
            cache: self.cache.clone(),
            config: self.config.clone(),
        }
    }
}

impl DbManager {
    /// create a DbManager instance
    pub fn new(config: &DbManagerConfig, shutdown: Arc<AtomicBool>) -> Result<DbManager, String> {
        //RocksDbConfig
        let db = RocksDb::new(&config.db_config, shutdown)?;
        let cache = ShardedCache::new(&config.cache_config);

        Ok(DbManager {
            enabled: config.enabled,
            name: config.name.clone(),
            db: Arc::new(db),
            cache: Arc::new(cache),
            config: config.clone(),
        })
    }

    /// get key as str
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<(Vec<u8>, bool), String> {
        if !self.enabled {
            return Err(String::from("DB is not enabled"));
        }

        if let Ok(val) = self.cache.get(&key) {
            return Ok((val, true));
        }

        match self.db.get(key) {
            Ok(value) => {
                if self.config.cache_config.cache_update_on_db_read {
                    if let Err(e) = self.cache.put(&key, &value) {
                        return Err(e.to_string());
                    }
                }
                Ok((value.to_vec(), false))
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// get key as str
    #[inline]
    pub fn get_key_val(&self, kv: &KeyVal) -> Result<(Vec<u8>, bool), String> {
        if !self.enabled {
            return Err(String::from("DB is not enabled"));
        }

        if let Ok(val) = self.cache.get_key_val(&kv) {
            return Ok((val, true));
        }

        match self.db.get(&kv.key) {
            Ok(value) => {
                if self.config.cache_config.cache_update_on_db_read {
                    let mut kv = kv.clone();
                    kv.val.extend_from_slice(&value);
                    if let Err(e) = self.cache.put_key_val(&kv) {
                        return Err(e.to_string());
                    }
                }
                Ok((value, false))
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// put the key val pair into database
    #[inline]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        if !self.enabled {
            return Err(String::from("DB is not enabled"));
        }
        self.db.put(&key, &val)?;

        if self.config.cache_config.cache_update_on_db_write {
            self.cache.put(&key, &val)?;
        }
        Ok(())
    }

    /// put the key val pair into database
    #[inline]
    pub fn put_key_val(&self, kv: &KeyVal) -> Result<(), String> {
        if !self.enabled {
            return Err(String::from("DB is not enabled"));
        }
        self.put(&kv.key, &kv.val)?;
        if self.config.cache_config.cache_update_on_db_write {
            self.cache.put_key_val(&kv)?;
        }
        Ok(())
    }

    /// delete they key in the db if found
    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        if !self.enabled {
            return Err(String::from("DB is not enabled"));
        }
        let _ = self.cache.delete(&key);
        self.db.delete(key)
    }

    /// delete they key in the db if found
    #[inline]
    pub fn delete_key_val(&self, kv: &KeyVal) -> Result<(), String> {
        if !self.enabled {
            return Err(String::from("DB is not enabled"));
        }
        let _ = self.cache.delete(&kv.key);
        self.db.delete(&kv.key)
    }

    pub fn backup_db(&self) -> Result<(), String> {
        if !self.enabled {
            return Err(format!("DB backup  is not enabled for DB: {}", self.name));
        }
        self.db.backup_db()
    }

    pub fn export_lru_keys(&self) -> Result<u64, String> {
        self.cache.export_keys()
    }
}
