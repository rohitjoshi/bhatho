/************************************************

   File Name: bhatho:keyval
   Author: Rohit Joshi <rohit.c.joshi@gmail.com>
   Date: 2019-02-17:15:15
   License: Apache 2.0

**************************************************/
use std::hash::Hasher;
use twox_hash::XxHash;
use crc16::{State, XMODEM};
use jumphash;


//key value structure
#[derive(Serialize, Deserialize, Debug)]
pub struct KeyVal {
    pub hash: u64,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub db_name: Vec<u8>,
    pub skip_db: bool,
    pub skip_cache: bool,
}

impl Clone for KeyVal {
    fn clone(&self) -> KeyVal {
        KeyVal {
            hash: self.hash,
            key: self.key.clone(),
            val: self.val.clone(),
            db_name: self.db_name.clone(),
            skip_db: self.skip_db,
            skip_cache: self.skip_cache,
        }
    }
}

impl KeyVal {

    #[inline]
    pub fn new(key: &[u8], val: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: val.to_vec(),
            db_name: vec![],
            skip_db: false,
            skip_cache: false,
        }
    }


    #[inline]
    pub fn new_with_db_name(db_name: &[u8], key: &[u8], val: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: val.to_vec(),
            db_name: db_name.to_vec(),
            skip_db: false,
            skip_cache: false,
        }
    }

    #[inline]
    pub fn new_with_hash(key_hash: u64, key: &[u8], val: &[u8]) -> KeyVal {
        KeyVal {
            hash: key_hash,
            key: key.to_vec(),
            val: val.to_vec(),
            db_name: vec![],
            skip_db: false,
            skip_cache: false,
        }
    }

    #[inline]
    pub fn new_with_key(key: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: vec![],
            db_name: vec![],
            skip_db: false,
            skip_cache: false,
        }
    }

    #[inline]
    pub fn new_with_db_key(db_name: &[u8], key: &[u8]) -> KeyVal {
        let hash = KeyVal::get_hash_code(&key);
        KeyVal {
            hash,
            key: key.to_vec(),
            val: vec![],
            db_name: db_name.to_vec(),
            skip_db: false,
            skip_cache: false,
        }
    }

    ///
   /// get the slot based on total slot count
    pub fn slot(&self, slot_count: usize) -> u64 {
        if slot_count == 1 {
            return 0;
        }
        KeyVal::gen_consistent_slot(self.hash, slot_count)
    }

    ///
  /// get the slot based on total slot count
    pub fn key_slot(key:&[u8], slot_count: usize) -> u64 {
        if slot_count == 1 {
            return 0;
        }
        let h = KeyVal::get_hash_code(key);
        KeyVal::gen_consistent_slot(h, slot_count)
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    #[inline]
    pub fn get_hash_code(key: &[u8]) -> u64 {
        let mut hasher = XxHash::with_seed(0);
        hasher.write(&key);
        hasher.finish()

        //State::<XMODEM>::calculate(key) as u64

        //let jh = jumphash::JumpHasher::new();
        //jh.slot(&key, self.config.num_shards as u32)
    }

    #[inline]
    fn get_slot_from_hash(hash: u64, slot_count: usize) -> u64 {
        KeyVal::gen_consistent_slot(hash, slot_count)
    }

    #[inline]
    fn gen_consistent_slot(hash:u64, slot_count: usize) -> u64 {

        let mut h = hash;
        let (mut b, mut j) = (-1i64, 0i64);
        while j < slot_count as i64 {
            b = j;
            h = h.wrapping_mul(2862933555777941757).wrapping_add(1);
            j = ((b.wrapping_add(1) as f64) * (((1u64 << 31) as f64) / (((h >> 33) + 1) as f64))) as i64;
        }
        b as u64
    }

    #[inline]
    pub fn get_slot_jumphash(key: &[u8], slot_count: usize) -> u64 {
        let jh = jumphash::JumpHasher::new();
        jh.slot(&key, slot_count as u32) as u64
    }



}

#[cfg(test)]
mod tests {
    //use crate::tests::rand::Rng;
    use super::*;
    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    #[test]
    fn test_get_hash_code() {
        let key = b"1234567890abcdefghijkl";

    }
}
