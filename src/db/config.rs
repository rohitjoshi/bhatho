/************************************************

   File Name: bhatho:db::config
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
use std::str;

use crate::cache::config::CacheConfig;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DbManagerConfig {
    pub enabled: bool,
    pub name: String,
    pub db_config: RocksDbConfig,
    pub cache_config: CacheConfig,
}

impl Default for DbManagerConfig {
    fn default() -> DbManagerConfig {
        let db_config = RocksDbConfig::default();
        let cache_config = CacheConfig::default();
        DbManagerConfig {
            enabled: true,
            name: "".to_string(),
            db_config,
            cache_config,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RocksDbConfig {
    pub enabled: bool,
    pub async_write: bool,
    pub num_async_writer_threads: usize,
    pub async_writer_threads_sleep_ms: u64,
    pub db_path: String,
    pub wal_dir: String,
    pub backup_path: String,
    pub backup_enabled: bool,
    pub max_open_files: i32,
    pub num_threads_parallelism: i32,
    pub create_if_missing: bool,
    pub pipelined_write: bool,
    pub min_count_for_batch_write: usize,
    pub write_buffer_size_mb: usize,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number: i32,
    pub max_background_compactions: i32,
    pub max_background_flushes: i32,
    pub block_size: usize,
    pub lru_cache_size_mb: usize,
    pub num_shard_bits: i32,
    pub disable_wal: bool,
    pub bloom_filter: bool,
    pub enable_statistics: bool,
    pub restore_from_backup_at_startup: bool,
    pub keep_log_file_while_restore: bool,
}

impl Default for RocksDbConfig {
    fn default() -> RocksDbConfig {
        RocksDbConfig {
            enabled: true,
            async_write: true,
            num_async_writer_threads: 1,
            async_writer_threads_sleep_ms: 250,
            db_path: "/tmp/kanudo_db".to_string(),
            wal_dir: "/tmp/kanudo_db/wal".to_string(),
            backup_path: "/tmp/kanudo_db_bkup".to_string(),
            backup_enabled: true,
            max_open_files: 5000,
            num_threads_parallelism: 2,
            create_if_missing: true,
            pipelined_write: true,
            min_count_for_batch_write: 100,
            write_buffer_size_mb: 512,
            max_write_buffer_number: 5,
            min_write_buffer_number: 2,
            max_background_compactions: 8,
            max_background_flushes: 8,
            block_size: 32768,
            lru_cache_size_mb: 10240,
            num_shard_bits: 12,
            disable_wal: false,
            bloom_filter: false,
            enable_statistics: true,
            restore_from_backup_at_startup: true,
            keep_log_file_while_restore: true,
        }
    }
}
