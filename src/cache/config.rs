use std::str;
use std::str::FromStr;
use std::collections::HashMap;

pub struct CacheManagerConfig {
    pub cache_configs: Vec<CacheConfig>
}
impl Default for CacheManagerConfig {
    fn default() -> CacheManagerConfig {
        let mut cache_configs = Vec::with_capacity(1);
        cache_configs.push(CacheConfig::default());
        CacheManagerConfig {
            cache_configs
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CacheConfig {
    pub enabled: bool,
    pub cache_capacity: usize,
    pub num_shards: usize,
    pub cache_update_on_db_read: bool,
    pub cache_update_on_db_write: bool,
    pub keys_dump_file: String,

}

impl Default for CacheConfig {
    fn default() -> CacheConfig {
        CacheConfig {
            enabled: true,
            cache_capacity: 1000000,
            num_shards: 1024,
            cache_update_on_db_read: true,
            cache_update_on_db_write: true,
            keys_dump_file: "/tmp/kanudo_lru_keys.dump".to_string(),
        }
    }
}