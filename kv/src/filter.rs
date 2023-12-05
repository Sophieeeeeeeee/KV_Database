#![allow(dead_code)]

use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
use twox_hash::XxHash64;

const PAGE_SIZE: usize = 4096;
const O_DIRECT: libc::c_int = 0x4000;
const SEEDS: [u64; 10] = [
    11798049322123270191,
    15539830439605854879,
    6578765718544580074,
    71743494464343003,
    9094065546985931996,
    17578418613310108530,
    3998834685102698833,
    17224146472807812495,
    13715473566396950222,
    7265912439666505101,
];

pub struct Bitmap {
    bits: Vec<u8>,
    size: u64,
}

impl Bitmap {
    pub fn new(size: u64) -> Self {
        let vec_size = (size + 7) / 8; // in bytes, ceil
        Bitmap {
            bits: vec![0; vec_size as usize],
            size,
        }
    }

    fn set(&mut self, idx: u64) {
        let byte_idx: usize = (idx / 8) as usize;
        let bit_idx = idx % 8;
        self.bits[byte_idx] |= 1u8 << bit_idx
    }

    fn is_set(&self, idx: u64) -> bool {
        // return true if set
        assert!(idx < self.size);
        let byte_idx: usize = (idx / 8) as usize;
        let bit_idx = idx % 8;
        (self.bits[byte_idx] & (1u8 << bit_idx)) != 0
    }

    fn reset(&mut self, new_size: u64) {
        self.bits.resize(new_size as usize, 0);
    }
}

pub trait BloomFilter {
    fn insert_key(&mut self, key: i64);
    fn check_key(&self, key: i64) -> bool;
}

impl BloomFilter for Bitmap {
    fn insert_key(&mut self, key: i64) {
        for seed in SEEDS {
            let mut hasher = XxHash64::with_seed(seed);
            key.hash(&mut hasher);
            let bit_idx = hasher.finish() % self.size;

            assert!(bit_idx < self.size);
            self.set(bit_idx);
        }
    }

    fn check_key(&self, key: i64) -> bool {
        for seed in SEEDS {
            let mut hasher = XxHash64::with_seed(seed);
            key.hash(&mut hasher);
            let bit_idx = hasher.finish() % self.size;

            assert!(bit_idx < self.size);
            if !self.is_set(bit_idx) {
                return false;
            }
        }
        true
    }
}

pub fn construct_filter(leaf_lst: &Vec<(i64, i64)>, bits_per_entry: &u8) -> Bitmap {
    let bitmap_size: u64 = (*bits_per_entry as usize * leaf_lst.len()) as u64;
    let mut bitmap = Bitmap::new(bitmap_size);
    for (key, _val) in leaf_lst {
        bitmap.insert_key(*key);
    }
    bitmap
}

fn serialize_filter(filename: &str, leaf_lst: &Vec<(i64, i64)>, bits_per_entry: &u8) {
    // 1. constuct bloom filter bit map
    let bitmap = construct_filter(leaf_lst, bits_per_entry);

    // 2. serialize to file:
    // 16 bytes of metadata: bitmap_size in bits (u64) + start page idx of btree(u64)
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(&(bitmap.size).to_be_bytes()); // 8 bytes
    let in_byte_size = (bitmap.size + 7) / 8; // in bytes, ceil
    let btree_idx = ((16 + in_byte_size) + PAGE_SIZE as u64 - 1) / PAGE_SIZE as u64; // ceil
    bytes.extend_from_slice(&(btree_idx).to_be_bytes()); // 8 bytes

    // bitmap
    bytes.extend_from_slice(&bitmap.bits);

    // pad rest of page with 0s
    let mut padding_size = 0;
    if bytes.len() % PAGE_SIZE != 0 {
        padding_size = PAGE_SIZE - (bytes.len() % PAGE_SIZE);
    }
    let padding = vec![0; padding_size];
    bytes.extend_from_slice(&padding);

    // write!
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .custom_flags(O_DIRECT)
        .open(filename)
        .expect("Filter Serializer: failed to create / append!");

    file.write_all(&bytes)
        .expect("Filter Serializer: file write failed!");
}

fn deserialize_filter(filename: &str) -> (Bitmap, usize) {
    // 1. read metadata + bitmap
    let mut file: File = OpenOptions::new()
        .read(true)
        .custom_flags(O_DIRECT)
        .open(filename)
        .expect("Filter Deserializer: open file failed!");

    let mut bytes = vec![0u8; 8];
    file.read_exact(&mut bytes)
        .expect("Filter Deserializer: file exact read failed!");
    let bitmap_size = u64::from_be_bytes(bytes.clone().try_into().unwrap());
    file.read_exact(&mut bytes)
        .expect("Filter Deserializer: file exact read failed!");
    let btree_idx = u64::from_be_bytes(bytes.clone().try_into().unwrap());
    let in_byte_size: usize = ((bitmap_size + 7) / 8) as usize; // in bytes, ceil

    let mut bitmap_bytes = vec![0u8; in_byte_size];
    file.read_exact(&mut bitmap_bytes)
        .expect("Filter Deserializer: file exact read failed!");

    // 2. construct Bitmap
    let bitmap = Bitmap {
        size: bitmap_size,
        bits: bitmap_bytes,
    };

    (bitmap, btree_idx as usize)
}

#[cfg(test)]
mod tests {
    use crate::filter::{
        construct_filter, /*deserialize_filter, serialize_filter,*/ Bitmap, BloomFilter,
    };
    // use std::fs::{create_dir_all, remove_dir_all};

    #[test]
    fn test_filter_insert_and_check() {
        let mut bitmap = Bitmap::new(200 * 10);
        let key1: i64 = 137;
        bitmap.insert_key(key1);
        assert!(bitmap.check_key(key1));
        let key2: i64 = 56;
        assert!(!bitmap.check_key(key2));
        bitmap.insert_key(key2);
        assert!(bitmap.check_key(key2));
        let key3: i64 = 178;
        assert!(!bitmap.check_key(key3));
    }

    #[test]
    fn test_filter_construction() {
        let mut lst: Vec<(i64, i64)> = Vec::new();
        for i in 0..=511 {
            lst.push((i, i));
        }
        let filter: Bitmap = construct_filter(&lst, &(10 as u8));
        assert!(filter.check_key(299 as i64));
        assert!(!filter.check_key(513 as i64));
    }

    // #[test]
    // fn test_filter_serde() {
    //     let db_name: String = "filterTestDB1".to_string();
    //     let folder_path: String = format!("./{}", db_name);
    //     create_dir_all(&folder_path).expect("Create dir all has failed!");
    //     let filename: String = format!("{}/testfile.bin", folder_path);

    //     let mut lst: Vec<(i64, i64)> = Vec::new();
    //     for i in 0..=511 {
    //         lst.push((i, i));
    //     }

    //     serialize_filter(&filename, &lst, &(10 as u8));
    //     let (filter, btree_idx) = deserialize_filter(&filename);

    //     assert!(filter.size == 512 * 10);
    //     assert!(!filter.check_key(&(512 as i64)));
    //     assert!(filter.check_key(&(511 as i64)));
    //     assert!(!filter.check_key(&(999 as i64)));
    //     assert!(filter.check_key(&(348 as i64)));

    //     assert!(btree_idx == 1);

    //     remove_dir_all(folder_path).expect("Remove dir all has failed!");
    // }
}
