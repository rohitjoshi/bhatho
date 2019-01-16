use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::hash::Hasher;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use twox_hash::{RandomXxHashBuilder, XxHash};

//key value structure
pub struct KeyVal {
    pub hash: u64,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub db_name: Vec<u8>,
}

impl Clone for KeyVal {
    fn clone(&self) -> KeyVal {
        KeyVal {
            hash: self.hash,
            key: self.key.clone(),
            val: self.val.clone(),
            db_name: self.db_name.clone(),
        }
    }
}

impl KeyVal {
    #[inline]
    pub fn new( key: &[u8], val: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: val.to_vec(),
            db_name: vec![]

        }
    }

    #[inline]
    pub fn new_with_db_name(db_name: &[u8],key: &[u8], val: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: val.to_vec(),
            db_name: db_name.to_vec()

        }
    }

    #[inline]
    pub fn new_with_hash(key_hash: u64, key: &[u8], val: &[u8]) -> KeyVal {
        KeyVal {
            hash: key_hash,
            key: key.to_vec(),
            val: val.to_vec(),
            db_name: vec![]
        }
    }

    #[inline]
    pub fn new_with_key(key: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: vec![],
            db_name: vec![]
        }
    }


    pub fn get_hash_code(key: &[u8]) -> u64 {
        let mut hasher = XxHash::with_seed(0);
        hasher.write(&key);
        hasher.finish()
    }
}