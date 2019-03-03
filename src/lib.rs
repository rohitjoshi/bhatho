/************************************************

   File Name: bhatho:lib
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
#![feature(test)]
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use regex;
use regex::Regex;
use std::str;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::db::config::DbManagerConfig;
use crate::db::db_manager::DbManager;
use crate::keyval::KeyVal;

pub mod cache;
pub mod db;
pub mod keyval;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegExMapping {
    extract_name_regex: String,
    new_db_name: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DbNameExtractor {
    pub enabled: bool,
    pub override_nonempty: bool,
    pub regex_mappings: Vec<RegExMapping>,
}

///
/// define a crate level config structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BhathoConfig {
    pub db_configs: Vec<DbManagerConfig>,
    pub db_name_extractor_from_key: DbNameExtractor,
}
impl Default for BhathoConfig {
    fn default() -> BhathoConfig {
        let mut db_configs = Vec::with_capacity(1);
        db_configs.push(DbManagerConfig::default());
        let db_name_extractor_from_key = DbNameExtractor {
            enabled: false,
            override_nonempty: false,
            regex_mappings: vec![],
        };
        BhathoConfig {
            db_configs,
            db_name_extractor_from_key,
        }
    }
}

///
/// Bhatho database instance
pub struct Bhatho {
    dbs: Arc<Vec<DbManager>>,
    config: BhathoConfig,
    regexs: Vec<(String, Regex)>,
}

impl Clone for Bhatho {
    #[inline]
    fn clone(&self) -> Bhatho {
        Bhatho {
            dbs: self.dbs.clone(),
            config: self.config.clone(),
            regexs: self.regexs.clone(),
        }
    }
}

impl Bhatho {
    ///
    /// extract table name from key
    ///
    fn extract_table_name_from_key(&self, kv: &KeyVal) -> Result<String, String> {
        if self.config.db_name_extractor_from_key.enabled
            && (kv.db_name.is_empty() || self.config.db_name_extractor_from_key.override_nonempty)
        {
            for (new_db_name, re) in &self.regexs {
                for caps in re.captures_iter(&String::from_utf8_lossy(&kv.key)) {
                    if let Some(_cap) = &caps.get(0) {
                        return Ok(new_db_name.to_string());
                    }
                }
            }
        }
        Err("not_found".to_string())
    }

    ///
    /// get shard logic is simple. mod of hash code with number of db instances.
    /// in future, we can improve by different criteria
    /// e.g Key suffix or prefix
    /// TODO: might want to convert into more efficient lookup compared to string compared
    /// may be hash table
    fn get_shard(&self, kv: &KeyVal) -> usize {
        let mut db_name = kv.db_name.clone();
        if let Ok(name) = self.extract_table_name_from_key(&kv) {
            db_name = name.as_bytes().to_vec();
        }
        if !db_name.is_empty() {
            for i in 0..self.dbs.len() {
                if self.dbs[i].name.as_bytes() == kv.db_name.as_slice() {
                    return i;
                }
            }
            //FIXME: Should we return error
        }
        KeyVal::get_hash_code(&kv.key) as usize % self.dbs.len()
    }

    pub fn new(config: &BhathoConfig, shutdown: Arc<AtomicBool>) -> Result<Bhatho, String> {
        let mut dbs = Vec::with_capacity(config.db_configs.len());
        for db_config in config.db_configs.iter() {
            let db_mgr = DbManager::new(db_config, shutdown.clone())?;
            dbs.push(db_mgr);
        }

        let mut regexs = Vec::new();

        if config.db_name_extractor_from_key.enabled {
            for mapping in &config.db_name_extractor_from_key.regex_mappings {
                let re = Regex::new(&mapping.extract_name_regex).unwrap();
                regexs.push((mapping.new_db_name.clone(), re));
            }
        }

        Ok(Bhatho {
            dbs: Arc::new(dbs),
            config: config.clone(),
            regexs,
        })
    }

    ///
    /// get the value for a given key
    pub fn get(&self, kv: &KeyVal) -> Result<(Vec<u8>, bool), String> {
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
    pub fn export_lru_keys(&self, db_name: &[u8]) -> Result<(), String> {
        info!("Exporting Lru Keys.  might take a while. Make sure instance remains up.");
        for db in self.dbs.iter() {
            let mut count = 0u64;
            if !db_name.is_empty() {
                if db.name.as_bytes() == db_name {
                    info!("Exporting keys for  db cache : {}", db.name);
                    count = db.export_lru_keys()?;
                }
            } else {
                info!("Exporting keys for  db cache : {}", db.name);
                count = db.export_lru_keys()?;
            }

            info!("{} Keys export completed for db cache: {}", count, db.name);
        }
        Ok(())
    }

    pub fn backup_db(&self, db_name: &[u8]) -> Result<(), String> {
        info!("Taking a backup.  might take a while. Make sure instance remains up.");

        for db in self.dbs.iter() {
            if !db_name.is_empty() {
                if db.name.as_bytes() == db_name {
                    info!("Taking back for db: {}", db.name);
                    db.backup_db()?;
                }
            } else {
                info!("Taking back for db: {}", db.name);
                db.backup_db()?;
            }
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

        let mut data: Vec<KeyVal> = Vec::with_capacity(total);
        let mut r_th = rand::thread_rng();

        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th
                .sample_iter(&Alphanumeric)
                .take(128)
                .collect::<String>();
            let kv = KeyVal::new(&key.as_bytes(), &val.as_bytes());
            data.push(kv);
        }
        let conf = BhathoConfig::default();
        let bhatho = Bhatho::new(&conf);
        if let Err(e) = bhatho {
            println!("Failed to init db. Error:{:?}", e);

            return;
        }

        let db = bhatho.unwrap();
        b.iter(|| {
            for (kv) in data.iter() {
                let mut val = db.put(kv);
            }
        });
    }
    #[bench]
    fn TenK_Read_bench(b: &mut Bencher) {
        let total = 10_000;

        let mut data: Vec<KeyVal> = Vec::with_capacity(total);
        let mut r_th = rand::thread_rng();

        for _ in 0..total {
            let key = r_th.sample_iter(&Alphanumeric).take(32).collect::<String>();
            let val = r_th
                .sample_iter(&Alphanumeric)
                .take(128)
                .collect::<String>();
            let kv = KeyVal::new(&key.as_bytes(), &val.as_bytes());
            data.push(kv);
        }
        let mut conf = BhathoConfig::default();
        conf.db_configs[0].db_config.db_path = "/tmp/bhatho_tmp".to_string();
        conf.db_configs[0].db_config.wal_dir = "/tmp/bhatho_tmp/wal".to_string();
        let bhatho = Bhatho::new(&conf);
        if let Err(e) = bhatho {
            println!("Failed to init db. Error:{:?}", e);

            return;
        }
        let db = bhatho.unwrap();
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
        let conf = BhathoConfig::default();
        let bhatho = Bhatho::new(&conf);
        if let Err(e) = bhatho {
            println!("Failed to init db. Error:{:?}", e);
            assert!(e.to_string() == "failed to init_db".to_string());
            return;
        }
        assert!(bhatho.is_ok() == true);
        let db = bhatho.unwrap();
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
