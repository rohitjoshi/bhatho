/************************************************

   File Name: bhatho:db::db_manager
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::cache::sharded_cache::ShardedCache;
use crate::db::config::DbManagerConfig;
use crate::db::rocks_db::RocksDb;
use crate::keyval::KeyVal;

/// DbManager
/// It is a wrapper around multiple database instances
pub struct DbManager {
    pub name: String,
    db: Option<Arc<RocksDb>>,
    cache: Arc<ShardedCache>,
    config: DbManagerConfig,
}

unsafe impl Send for DbManager {}

unsafe impl Sync for DbManager {}

/// Clone the instance
impl Clone for DbManager {
    #[inline]
    fn clone(&self) -> DbManager {
        DbManager {
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

        let db = if config.rocks_db_config.enabled {
            let rocks_db = RocksDb::new(&config.rocks_db_config, shutdown)?;
            Some(Arc::new(rocks_db))
        } else {
            None
        };
        let cache = ShardedCache::new(&config.cache_config);


        Ok(DbManager {
            name: config.name.clone(),
            db,
            cache: Arc::new(cache),
            config: config.clone(),
        })
    }

    /// get key as str
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Option<(Vec<u8>, bool)>, String> {
        debug!("db_manager:get()");
        if let Some(val) = self.cache.get(&key) {
            debug!("db_manager:get value received from cache");
            return Ok(Some((val, true)));
        }
        debug!("db_manager:get_key_val not found in cache");
        if self.db.is_none() {
            return Ok(None);
        }
        match self.db.as_ref().unwrap().get(key) {
            Ok(Some(value)) => {
                debug!("db_manager:get value received from db");
                if self.config.cache_config.cache_update_on_db_read {
                    debug!("db_manager:get value received from db and updating cache");
                    let _ = self.cache.put(&key, &value);
                }
                Ok(Some((value, false)))
            }
            Ok(None) => {
                debug!("db_manager:get value not found from db");
                Ok(None)
            }
            Err(e) => {
                debug!("db_manager: from db get error: {:?}", e);
                Err(e.to_string())
            }
        }
    }

    /// get key as str
    #[inline]
    pub fn get_key_val(&self, kv: &KeyVal) -> Result<Option<(Vec<u8>, bool)>, String> {
        debug!("db_manager:get_key_val()");
        if let Some(val) = self.cache.get_key_val(&kv) {
            debug!("db_manager:get_key_val value received from cache");
            return Ok(Some((val, true)));
        }

        debug!("db_manager:get_key_val not found in cache");
        if self.db.is_none() {
            return Ok(None);
        }
        match self.db.as_ref().unwrap().get(&kv.key) {
            Ok(Some(value)) => {
                debug!("db_manager:get_key_val value received from db");
                if self.config.cache_config.cache_update_on_db_read {
                    debug!("db_manager:get_key_val value received from db and updating cache");
                    let _ = self.cache.put_key_val(&kv, &value);
                }
                Ok(Some((value, false)))
            }
            Ok(None) => {
                debug!("db_manager:get_key_val value not found from db");
                Ok(None)
            }
            Err(e) => {
                debug!("db_manager:get_key_val from db error: {:?}", e);
                Err(e.to_string())
            }
        }
    }

    /// put the key val pair into database
    #[inline]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        debug!("db_manager:put");
        if self.db.is_some() {
            self.db.as_ref().unwrap().put(&key, &val)?;
        }
        debug!("db_manager:put success");

        if self.config.cache_config.cache_update_on_db_write {
            debug!("db_manager:put success. updating cache");
            self.cache.put(&key, &val)?;
        }
        Ok(())
    }

    /// put the key val pair into database
    #[inline]
    pub fn put_key_val(&self, kv: &KeyVal) -> Result<(), String> {
        debug!("db_manager:put_key_val");
        if self.db.is_some() {
            self.db.as_ref().unwrap().put(&kv.key, &kv.val)?;
        }
        debug!("db_manager:put_key_val success");
        if self.config.cache_config.cache_update_on_db_write {
            debug!("db_manager:put_key_val success. updating cache");
            self.cache.put(&kv.key, &kv.val)?;
        }
        Ok(())
    }

    /// delete they key in the db if found
    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        let _ = self.cache.delete(&key);
        if self.db.is_some() {
            return self.db.as_ref().unwrap().delete(key);
        }
        Ok(())
    }

    /// delete they key in the db if found
    #[inline]
    pub fn delete_key_val(&self, kv: &KeyVal) -> Result<(), String> {
        let _ = self.cache.delete(&kv.key);
        if self.db.is_some() {
            return self.db.as_ref().unwrap().delete(&kv.key);
        }
        Ok(())
    }

    pub fn backup_db(&self) -> Result<(), String> {
        if self.db.is_some() {
            return self.db.as_ref().unwrap().backup_db();
        }
        Ok(())
    }

    pub fn export_lru_keys(&self) -> Result<u64, String> {
        self.cache.export_keys()
    }
}
