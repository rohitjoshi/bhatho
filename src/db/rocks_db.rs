/************************************************

   File Name: bhatho:db::rocks_db
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
use crossbeam_channel as mpsc;
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, DB as rocks_db, DBCompressionType,
              SliceTransform, WriteBatch};
use rocksdb::backup::{BackupEngine, BackupEngineOptions};
use rocksdb::Options as rocks_options;


use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

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

    ///
    /// Create rocks_db_options
    ///
    fn create_rocks_db_options(rocks_config: &RocksDbConfig) -> Result<rocks_options, String> {

        let mut opts = rocks_options::default();
        opts.set_max_open_files(rocks_config.max_open_files);
        opts.increase_parallelism(rocks_config.num_threads_parallelism);
        opts.create_if_missing(rocks_config.create_if_missing);
        opts.set_compression_type(DBCompressionType::None); //Lz4
        //opts.enable_pipelined_write(rocks_config.pipelined_write);
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

        Ok(opts)


    }
    /// initialize rocks db options and create a new db instance
    fn init_rocks_db(rocks_config: &RocksDbConfig) -> Result<rocks_db, String> {
        info!("Creating RocksDB instance");

        let opts = RocksDb::create_rocks_db_options(&rocks_config)?;
        match rocks_db::open(&opts, &rocks_config.db_path) {
            Ok(db) => {
               Ok(db)
            }
            Err(e) => {
                error!("Failed to open rockdb database. Error:{:?}", e);
               Err(e.to_string())
            }
        }
    }


    /// Write to database async
    /// It reads from the channel
    fn write_to_db(
        db_config: RocksDbConfig,
        db: Arc<rocks_db>,
        receiver: mpsc::Receiver<KeyVal>,
        shutdown: Arc<AtomicBool>
    ) {
        loop {
            let data: Vec<KeyVal> = receiver.try_iter().collect();

            //timeout, no data received. let's sleep
            if data.is_empty() {
                if shutdown.load(Ordering::SeqCst) {
                    info!("Shutdown received. Exiting while loop");
                    return;
                }
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
            } else if let Err(e) = db.write(batch) {
                error!("Failed to batch write to RocksDB. Error:{:?}", e);
            }
        }
    }

    /// create a RocksDB instance from the config
    pub fn new(config: &RocksDbConfig,  shutdown: Arc<AtomicBool>) -> Result<RocksDb, String> {


        if config.restore_from_backup_at_startup {
            if let Ok(mut backup_engine) = RocksDb::create_backup_engine(&config) {
                let mut restore_option = rocksdb::backup::RestoreOptions::default();
                restore_option.set_keep_log_files(config.keep_log_file_while_restore);
                let mut wal_dir = config.wal_dir.clone();

                if wal_dir.is_empty() {
                    wal_dir = config.db_path.clone();
                }
                if let Err(e) = backup_engine.restore_from_latest_backup(&config.db_path, &wal_dir, &restore_option) {
                    error!("Failed to restore from the backup. Error:{:?}", e);
                    return Err(e.to_string());
                }
                info!("Restoring DB from a backup path: {}", config.backup_path);
            } else {

            }
        }else {
            info!("Initializing DB from a path: {}", config.db_path);
        }

        let db = Arc::new(RocksDb::init_rocks_db(&config)?);

        let (tx, rx) = mpsc::unbounded::<KeyVal>();


        if config.async_write {
            for _i in 0..config.num_async_writer_threads {
                let config_clone = config.clone();
                let db_clone = db.clone();
                let rx = rx.clone();
                let shutdown = shutdown.clone();
                thread::spawn(move || {
                    RocksDb::write_to_db(config_clone, db_clone, rx, shutdown);
                });
            }
        }


        Ok(RocksDb {
            db,
            sender: tx,
            config: config.clone(),
        })
    }

    fn create_backup_engine(config: &RocksDbConfig) -> Result<BackupEngine, String> {
        if !config.backup_enabled {
            info!("Backup is not enabled for DB with path: {}. ", config.backup_path);
            return Err("Backup is not enabled.".to_string())
        }else if config.backup_path.is_empty(){
            error!("Backup path: {} is empty. Not enabling backup engine", config.backup_path);
            return Err("Backup path is empty. Not enabling backup engine".to_string())
        }
        let backup_opts = BackupEngineOptions::default();
        match BackupEngine::open(&backup_opts, &config.backup_path) {
            Err(e) => {
                error!("Failed to open backup engine for path: {}. Error:{:?}", config.backup_path, e);
                Err(e.to_string())
            }
            Ok(backup_engine) => {
                info!("Successfully opened backup engine for path: {}", config.backup_path);
                Ok(backup_engine)
            }
        }
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
                Err(e.to_string())
            }
        }
    }


    #[inline]
    fn put_async(&self, key: &[u8], val: &[u8]) -> Result<(), String> {
        let key_val = KeyVal::new(&key, &val);
        match self.sender.send(key_val) {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(e.to_string())
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

        if let Ok(mut backup_engine) = RocksDb::create_backup_engine(&self.config) {

            if let Err(e) = backup_engine.create_new_backup(&self.db) {
                error!("Failed to purge old backups for DB with path: {}. Error:{:?}", self.config.backup_path, e);
                return Err(e.to_string());
            }
            info!("Purged old backup for DB Path: {},  Backup Path: {}.",
                  self.config.db_path, self.config.backup_path);
            Ok(())

            /*let res = self.backup_engine.map(|mut be| {
                if let Err(e) = be.create_new_backup(&self.db) {
                    {
                        error!("Failed to create a new backup using path: {}. Error:{:?}", self.config.backup_path, e);
                        return Err(e.to_string());
                    }
                }else {
                    info!("Backup completed. DB Path: {},  Backup Path: {}",
                          self.config.db_path, self.config.backup_path);
                    return Ok(());
                }
            });
            res.unwrap()*/

        }else {
            Err("Backup Engine was not initialized".to_string())
        }

    }

    pub fn purge_old_backup(&self, num_backups_to_keep: usize) -> Result<(), String>{

        if let Ok(mut backup_engine) = RocksDb::create_backup_engine(&self.config) {

            if let Err(e) = backup_engine.purge_old_backups(num_backups_to_keep) {
                error!("Failed to purge old backups for DB with path: {}. Error:{:?}", self.config.backup_path, e);
                return Err(e.to_string());
            }
            info!("Purged old backup for DB Path: {},  Backup Path: {}.",
                  self.config.db_path, self.config.backup_path);
            Ok(())
        }else {
            Err("Backup Engine was not initialized".to_string())
        }
    }
}

