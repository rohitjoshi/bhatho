#![feature(test)]
#[macro_use] extern crate log;
#[macro_use] extern crate serde_derive;


use std::str;
use std::sync::Arc;

use crate::cache::sharded_cache::ShardedCache;
use crate::cache::config::CacheConfig;
use crate::db::config::DbManagerConfig;
use crate::db::db_manager::DbManager;
use crate::keyval::KeyVal;

pub mod cache;
pub mod db;
pub mod keyval;


///
/// define a crate level config structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KanudoConfig {
    pub db_configs: Vec<DbManagerConfig>,


}
impl Default for KanudoConfig {
    fn default() -> KanudoConfig {
        let mut db_configs = Vec::with_capacity(1);
        db_configs.push(DbManagerConfig::default());
        KanudoConfig {
            db_configs

        }
    }
}


///
/// Kanudo database instance
pub struct Kanudo {
    dbs: Arc<Vec<DbManager>>,
    config: KanudoConfig
}

impl Clone for Kanudo {
    #[inline]
    fn clone(&self) -> Kanudo {
        Kanudo {
            dbs: self.dbs.clone(),
            config: self.config.clone(),


        }
    }
}

impl Kanudo {

    ///
    /// get shard logic is simple. mod of hash code with number of db instances.
    /// in future, we can improve by different criteria
    /// e.g Key suffix or prefix
    /// TODO: might want to convert into more efficient lookup compared to string compared
    /// may be hash table
    fn get_shard(&self, kv: &KeyVal) -> usize {
        if !kv.db_name.is_empty() {
            for i in 0..self.dbs.len() {
                if self.dbs[i].name.as_bytes() == kv.db_name.as_slice() {
                    return i;
                }
            }
        }
        KeyVal::get_hash_code(&kv.key) as usize % self.dbs.len()
    }



    pub fn new (config: & KanudoConfig) -> Result<Kanudo, String> {

        let mut dbs = Vec::with_capacity(config.db_configs.len());
        for ref db_config in config.db_configs.iter() {
            let db_mgr = DbManager::new(db_config)?;
            dbs.push(db_mgr);
        }

        Ok(Kanudo{
            dbs: Arc::new(dbs),
            config:config.clone()
        })

    }

    ///
    /// get the value for a given key
    pub fn get(&self, kv: &KeyVal) -> Result<Vec<u8>, String> {

        self.dbs[self.get_shard(&kv)].get_key_val(&kv)


    }
    ///
    /// put the key, val pair to DB and Lru Cache
    pub fn put(&self, kv: &KeyVal) -> Result<(), String> {
        self.dbs[self.get_shard(&kv)].put_key_val(&kv)
    }

    ///
    /// delete the key-val pair from db and lru cache for a given key
    pub fn delete(&self, kv: &KeyVal) -> Result<(), String> {
        self.dbs[self.get_shard(&kv)].delete_key_val(&kv)
    }

    ///
    /// Export all the Keys from LRU Cache to a file path configured in the cache mgr
    pub fn export_lru_keys(&self) -> Result<(), String> {
        for ref db in self.dbs.iter() {
            info!("Exporting keys for  db cache : {}", db.name);
            db.export_lru_keys()?;
            info!("Keys export completed for db cache: {}", db.name);
        }
        Ok(())
    }



    pub fn backup_db(&self) -> Result<(), String> {

        info!("Taking a backup.  might take a while. Make sure instance remains up.");

        for ref db in self.dbs.iter() {
            info!("Taking back for db: {}", db.name);

            db.backup_db()?;
            info!("Backup completed for db: {}", db.name);
        }
        info!("DB Backup completed");
        Ok(())
    }
}

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
    fn TenK_Write_bench(b: &mut Bencher) {
        let total = 10_000;

        let mut data: Vec<KeyVal>= Vec::with_capacity(total);
        let mut r_th = rand::thread_rng();

        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(128)
                .collect::<String>();
            let kv = KeyVal::new(&key.as_bytes(), &val.as_bytes());
            data.push(kv);
        }
        let conf = KanudoConfig::default();
        let kanudo = Kanudo::new(&conf);
        if let Err(e) = kanudo {
            println!("Failed to init db. Error:{:?}", e);

            return;
        }

        let db = kanudo.unwrap();
        b.iter(|| {
            for (kv) in data.iter() {
                let mut val = db.put(kv);
            }
        });

    }
    #[bench]
    fn TenK_Read_bench(b: &mut Bencher) {
        let total = 10_000;

        let mut data: Vec<KeyVal>= Vec::with_capacity(total);
        let mut r_th = rand::thread_rng();

        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th.sample_iter(&Alphanumeric)
                .take(128)
                .collect::<String>();
            let kv = KeyVal::new(&key.as_bytes(), &val.as_bytes());
            data.push(kv);
        }
        let mut conf = KanudoConfig::default();
        conf.db_configs[0].db_config.db_path="/tmp/kanudo_tmp".to_string();
        conf.db_configs[0].db_config.wal_dir="/tmp/kanudo_tmp/wal".to_string();
        let kanudo = Kanudo::new(&conf);
        if let Err(e) = kanudo {
            println!("Failed to init db. Error:{:?}", e);

            return;
        }
        let db = kanudo.unwrap();
        for (kv) in data.iter() {
            let mut val = db.put(&kv);
        }


        b.iter(|| {
            for (kv) in data.iter() {
                let mut val = db.get(&kv);
            }
        });

    }

    #[test]
    fn init_db_test() {
        let conf = KanudoConfig::default();
        let kanudo = Kanudo::new(&conf);
        if let Err(e) = kanudo {
            println!("Failed to init db. Error:{:?}", e);
            assert!(e.to_string() == "failed to init_db".to_string());
            return;
        }
        assert!(kanudo.is_ok() == true);
        let db =kanudo.unwrap();
        let key = b"test_key";
        let val = b"test_val";
        let kv = KeyVal::new(key, val);
        db.put(&kv);
        let res = db.get(&kv);
        assert!(res.is_ok() == true);
        let v2 = res.unwrap();
        assert!(val.to_vec() == v2);
    }




}
