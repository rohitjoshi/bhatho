use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel as mpsc;
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, DB as rocks_db, DBCompressionType,
              SliceTransform, WriteBatch, WriteOptions};
use rocksdb::backup::{BackupEngine, BackupEngineOptions};
use rocksdb::Options as rocks_options;

use crate::db::config::RocksDbConfig;
use crate::keyval::KeyVal;

//TODO Add support for column family
pub struct RocksDb {
    pub db: Arc<rocks_db>,
    pub sender: mpsc::Sender<KeyVal>,
    pub config: RocksDbConfig,

}

//using single thread loop , so it is safe
//unsafe impl Send for Store {}
//unsafe impl Sync for Store {}


/// Clone the instance
impl Clone for RocksDb {
    #[inline]
    fn clone(&self) -> RocksDb {
        RocksDb {
            db: self.db.clone(),
            sender: self.sender.clone(),
            config: self.config.clone(),

        }
    }
}

impl RocksDb {
    /// initialize rocks db options and create a new db instance
    fn init_rocks_db(rocks_config: &RocksDbConfig) -> Result<rocks_db, String> {
        info!("Creating RocksDB instance");
        let mut opts = rocks_options::default();
        opts.set_max_open_files(rocks_config.max_open_files);
        opts.increase_parallelism(rocks_config.num_threads_parallelism);
        opts.create_if_missing(rocks_config.create_if_missing);
        opts.set_compression_type(DBCompressionType::None); //Lz4
        opts.enable_pipelined_write(rocks_config.pipelined_write);
        if rocks_config.enable_statistics {
            opts.enable_statistics();
        }
        opts.set_write_buffer_size(rocks_config.write_buffer_size_mb * 1024 * 1024); // 128mb
        opts.set_max_write_buffer_number(rocks_config.max_write_buffer_number);
        opts.set_min_write_buffer_number(rocks_config.min_write_buffer_number);
        opts.set_max_background_compactions(rocks_config.max_background_compactions);
        opts.set_max_background_flushes(rocks_config.max_background_flushes);
        // opts.set_allow_os_buffer(false);
        opts.set_allow_concurrent_memtable_write(true);

        if rocks_config.lru_cache_size_mb > 0 {
            opts.set_table_cache_num_shard_bits(rocks_config.num_shard_bits);
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_size(rocks_config.block_size);
            let prefix_extractor = SliceTransform::create_fixed_prefix(3);
            opts.set_prefix_extractor(prefix_extractor);
            block_opts.set_index_type(BlockBasedIndexType::HashSearch);

            block_opts.set_lru_cache(rocks_config.lru_cache_size_mb * 1024 * 1024); //1GB:  In prod, it should be 64GB
            block_opts.set_cache_index_and_filter_blocks(true);
            if rocks_config.bloom_filter {
                block_opts.set_bloom_filter(10, true);
            }

            opts.set_block_based_table_factory(&block_opts);
        }
        if !rocks_config.wal_dir.is_empty() {
            opts.set_wal_dir(&rocks_config.wal_dir);
        }

        match rocks_db::open(&opts, &rocks_config.db_path) {
            Ok(db) => {
                return Ok(db);
            }
            Err(e) => {
                error!("Failed to open rockdb database. Error:{:?}", e);
                return Err(e.to_string());
            }
        }
    }


    /// Write to database async
    /// It reads from the channel
    fn write_to_db(
        db_config: RocksDbConfig,
        db: Arc<rocks_db>,
        receiver: mpsc::Receiver<KeyVal>,
    ) {
        loop {
            let data: Vec<KeyVal> = receiver.try_iter().collect();

            //timeout, no data received. let's sleep
            if data.is_empty() {
                thread::sleep(Duration::from_millis(
                    db_config.async_writer_threads_sleep_ms,
                ));
                continue;
            }

            //we got data, write to db as a single record
            if data.len() < db_config.min_count_for_batch_write {
                for kv in data.iter() {
                    if let Err(e) = db.put(&kv.key, &kv.val) {
                        error!("Failed to batch write to RocksDB. Error:{:?}", e);
                    }
                }
                continue;
            }
            // write data as batch
            let mut batch = WriteBatch::default();
            for kv in data.iter() {
                if let Err(e) = batch.put(&kv.key, &kv.val) {
                    error!(
                        "Failed to add into the batch for writing to RocksDB. Error:{:?}",
                        e
                    );
                }
            }

            if db_config.disable_wal {
                if let Err(e) = db.write_without_wal(batch) {
                    error!("Failed to batch write to RocksDB. Error:{:?}", e);
                }
            } else {
                if let Err(e) = db.write(batch) {
                    error!("Failed to batch write to RocksDB. Error:{:?}", e);
                }
            }
        }
    }

    /// create a RocksDB instance from the config
    pub fn new(config: &RocksDbConfig) -> Result<RocksDb, String> {
        let db = Arc::new(RocksDb::init_rocks_db(&config)?);

        let (tx, rx) = mpsc::unbounded::<KeyVal>();


        if config.async_write {
            for _i in 0..config.num_async_writer_threads {
                let config_clone = config.clone();
                let db_clone = db.clone();
                let rx = rx.clone();
                thread::spawn(move || {
                    RocksDb::write_to_db(config_clone, db_clone, rx);
                });
            }
        }


        Ok(RocksDb {
            db,
            sender: tx,
            config: config.clone(),
        })
    }

    /// get key as str
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, String> {
        match self.db.get(key) {
            Ok(Some(value)) => {
                Ok(value.to_vec())
            }
            Ok(None) => Err(String::from("not found")),
            Err(e) => Err(e.to_string()),
        }
    }

    #[inline]
    pub fn put(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        if self.config.async_write {
            self.put_async(&key, &val)
        } else {
            match self.db.put(key, val) {
                Ok(_r) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }

    #[inline]
    pub fn put_key_val(&self, key_val: &KeyVal) -> Result<(), String> {
        if self.config.async_write {
            self.put_key_val_async(key_val)
        } else {
            match self.db.put(&key_val.key, &key_val.val) {
                Ok(_r) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        }
    }

    #[inline]
    fn put_key_val_async(&self, key_val: &KeyVal) -> Result<(), String> {
        match self.sender.send(key_val.clone()) {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }


    #[inline]
    fn put_async(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        let key_val = KeyVal::new(&key, &val);
        match self.sender.send(key_val) {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }

    #[inline]
    pub fn delete(&self, key: &[u8]) -> Result<(), String> {
        match self.db.delete(key) {
            Ok(_r) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    pub fn backup_db(&self) -> Result<(), String> {
        if !self.config.backup_enabled {
            debug!("Backup is not enabled.")
        }
        let backup_opts = BackupEngineOptions::default();
        match BackupEngine::open(&backup_opts, &self.config.backup_path) {
            Err(e) => {
                error!("Failed to open backup engine for path: {}. Error:{:?}", self.config.backup_path, e);
                return Err(e.to_string());
            }
            Ok(mut backup_engine) => {
                info!("Successfully opened backup engine for path: {}", self.config.backup_path);
                if let Err(e) = backup_engine.create_new_backup(&self.db) {
                    error!("Failed to create a new backup using path: {}. Error:{:?}", self.config.backup_path, e);
                }
                info!("Backup completed. DB Path: {},  Backup Path: {}",
                      self.config.db_path, self.config.backup_path);
            }
        }

        Ok(())
    }
}

