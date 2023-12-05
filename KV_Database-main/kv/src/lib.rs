mod buffer;
mod filter;
mod memtable;
mod serde;
mod storage;

use crate::memtable::Memtable;
use crate::storage::{AppendOnlyLog, BTree, DiskStorage, LSMTree};
use std::collections::HashMap;
use std::fs::{read_dir, remove_dir_all};
use std::path::Path;

/// Struct for the `Client`.
pub struct Client {
    /// The name of the DB.
    name: String,
    /// The memtable of the DB.
    memtable: Memtable,
    /// The current size of the memtable.
    memtable_size: u32,
    /// The number of ssts in the DB.
    sst_count: u32,
    /// The storage type of the DB (DiskStorage type).
    storage: Box<dyn DiskStorage>,
    /// If the DB should be cleaned up on close.
    cleanup: bool,
}

/// Struct for the `KVConfig`.
pub struct KVConfig {
    /// The memtable size to be used for the DB.
    memtable_size: u32,
    /// The buffer pool size to be used for the DB.
    bufferpool_size: usize,
    /// If the DB should be cleaned up on close.
    cleanup: bool,
    /// The storage type to be used for the DB.
    storage_type: StorageType,
}

// Implementation for the `KVConfig`.
impl KVConfig {
    /// Setting the memtable size.
    /// # Arguments
    /// * `size` - The memtable size wanted.
    pub fn memtable_size(mut self, size: u32) -> Self {
        self.memtable_size = size;
        self
    }
    /// Setting the buffer pool size.
    /// # Arguments
    /// * `size` - The buffer pool size wanted.
    pub fn bufferpool_size(mut self, size: usize) -> Self {
        self.bufferpool_size = size;
        self
    }
    /// Setting the storage type.
    /// # Arguments
    /// * `storage` - The storage type wanted.
    pub fn storage_type(mut self, storage: StorageType) -> Self {
        self.storage_type = storage;
        self
    }
    /// Setting if the DB should be cleaned up on close.
    /// # Arguments
    /// * `cleanup` - `true` or `false` if DB should be cleaned up on close.
    pub fn cleanup(mut self, cleanup: bool) -> Self {
        self.cleanup = cleanup;
        self
    }
}

// Special default implementation of the `KVConfig`.
impl Default for KVConfig {
    /// Default implementation of the `KVConfig`.
    /// Sets the memtable size and buffer pool size to 256 KV pairs, clean up to `false`, and storage type to append only log.
    fn default() -> Self {
        Self {
            memtable_size: 256,
            bufferpool_size: 256,
            cleanup: false,
            storage_type: StorageType::AppendOnlyLog,
        }
    }
}

/// Struct for the `StorageType`.
pub enum StorageType {
    AppendOnlyLog,
    BTree,
    LSMTree,
}

// Implementation for the `Client`.
impl Client {
    /// Creating a new `Client` with `name` and `config`.
    /// # Arguments
    /// * `name` - The name of the new `Client`.
    /// * `config` - A `KVConfig` object to set the values of the new `Client`.
    pub fn open(name: String, config: KVConfig) -> Self {
        let mut count: u32 = 0;
        let db_exists: bool = Path::new(&name).exists();
        if db_exists {
            count = match read_dir(&name) {
                Ok(entries) => {
                    let file_count = entries
                        .filter_map(Result::ok)
                        .filter(|entry| {
                            entry
                                .metadata()
                                .map(|metadata| metadata.is_file())
                                .unwrap_or(false)
                        })
                        .count();
                    file_count as u32
                }
                Err(_) => {
                    panic!("Should not get here!");
                }
            };
        }

        Self {
            name: name.clone(),
            memtable: Memtable::new(),
            memtable_size: config.memtable_size,
            sst_count: count,
            storage: match config.storage_type {
                StorageType::AppendOnlyLog => Box::new(AppendOnlyLog::new(name)),
                StorageType::BTree => Box::new(BTree::new(name, config.bufferpool_size)),
                StorageType::LSMTree => Box::new(LSMTree::new(
                    name,
                    config.bufferpool_size,
                    config.memtable_size,
                )),
            },
            cleanup: config.cleanup,
        }
    }

    /// Insert `key` and `value` into the `Client` DB.
    /// # Arguments
    /// * `self` - A mutable ref to the `Client` object to insert a new KV pair.
    /// * `key` - The new key to add.
    /// * `value` - The new value to add.
    pub fn put(&mut self, key: i64, value: i64) {
        self.memtable.put(key, value);
        if self.memtable.size() >= self.memtable_size {
            self.flush();
        }
    }

    /// Get the value corresponding to a `key` from the `Client` DB.
    /// # Arguments
    /// * `self` - A mutable ref to the `Client` object to get a value.
    /// * `key` - The key who's value is searched.
    pub fn get(&mut self, key: i64) -> Option<i64> {
        let result = self.memtable.get(key).or_else(|| self.storage.get(key));

        if result.map_or(false, |a| a == i64::MIN) {
            return None;
        }
        result
    }

    /// Scan the `Client` DB on a range of keys from `start` to `end` INCLUSIVE.
    /// # Arguments
    /// * `self` - A mutable ref to the `Client` object to scan for values.
    /// * `start` - The start key range of the scan.
    /// * `end` - The end key range of the scan.
    pub fn scan(&mut self, start: i64, end: i64) -> Vec<(i64, i64)> {
        if start > end {
            return Vec::new();
        }

        let mut kv_hash: HashMap<i64, i64> = HashMap::new();

        self.memtable.scan(start, end, &mut kv_hash);
        self.storage.scan(start, end, &mut kv_hash);

        kv_hash.into_iter().filter(|a| a.1 != i64::MIN).collect()
    }

    /// Close the `Client` DB. Flush if necessary.
    ///  # Arguments
    /// * `self` - A mutable ref to the `Client` object to flush it.
    pub fn close(&mut self) {
        if self.memtable.size() > 0 {
            self.flush();
        }
    }

    /// Flush the memtable into an SST.
    /// # Arguments
    /// * `self` - A mutable ref to the `Client` object to flush it.
    fn flush(&mut self) {
        let output_lst: Vec<(i64, i64)> = self.memtable.scan_all();

        self.storage.flush(self.sst_count, output_lst);

        self.sst_count += 1;
        self.memtable = Memtable::new();
    }

    pub fn delete(&mut self, key: i64) {
        self.memtable.put(key, i64::MIN)
    }

    pub fn update(&mut self, key: i64, value: i64) {
        self.memtable.put(key, value)
    }
}

// Special implementation of the drop function for the `Client`.
impl Drop for Client {
    /// Drop the `Client` DB. Close it and clean up if necessary.
    /// # Argument
    /// * `self` - A mutable ref to the `Client` object to drop.
    fn drop(&mut self) {
        self.close();

        if self.cleanup {
            let _ = remove_dir_all(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    mod binary_tree {
        mod get_value {
            use crate::{Client, KVConfig};
            use std::fs::create_dir_all;

            #[test]
            fn test_get_from_ssts() {
                let mut kv: Client = Client::open(
                    "getTestDB1".to_string(),
                    KVConfig::default()
                        .storage_type(crate::StorageType::AppendOnlyLog)
                        .cleanup(true),
                );

                let folder_path: &str = "./getTestDB1/";
                create_dir_all(folder_path).expect("Create dir all has failed!");

                for i in 0..200 {
                    kv.put(i as i64, (i * 2) as i64);
                }

                let key1: i64 = 12;
                let key2: i64 = 110;

                assert_eq!(Some(24), kv.get(key1));
                assert_eq!(Some(220), kv.get(key2));
            }
        }

        mod flush {
            use crate::Client;
            use crate::KVConfig;
            use std::fs::create_dir_all;

            #[test]
            fn test_memtable_flush() {
                let db_name: String = "flushTestDB1".to_string();
                let folder_path_string: String = format!("./{}/", db_name);
                let folder_path: &str = &folder_path_string.as_str();

                create_dir_all(folder_path).expect("Create dir all has failed!");

                let mut kv: Client = Client::open(
                    db_name,
                    KVConfig::default()
                        .storage_type(crate::StorageType::AppendOnlyLog)
                        .cleanup(true),
                );
                for i in 0..=98 {
                    kv.memtable.put(i, i);
                }
            }
        }

        // mod scan {
        //     use crate::Client;
        //     use std::fs::{create_dir_all, remove_dir, remove_file};

        //     #[test]
        //     fn test_scan_from_tree_and_sst() {
        //         let mut kv: Client =
        //             Client::open("scanTestDB1".to_string(), 100, crate::StorageType::SST);

        //         let folder_path: &str = "./scanTestDB1/";
        //         create_dir_all(folder_path).expect("Create dir all has failed!");

        //         for i in 0..200 {
        //             kv.put(i as i64, i * 2 as i64);
        //         }

        //         let output_lst = kv.scan(39, 167);

        //         let mut j: i64 = 39;
        //         for kv in output_lst.iter() {
        //             assert_eq!(*kv, (j, j * 2));
        //             j += 1;
        //         }
        //         assert!(j == 168);

        //         for i in 0..kv.sst_num {
        //             remove_file(format!("{}output_{}.bin", folder_path, i))
        //                 .expect("Remove file has failed!");
        //         }
        //         remove_dir(folder_path).expect("Remove dir has failed!");
        //     }

        //     #[test]
        //     fn test_scan_from_tree_and_sst_newer_values_in_tree() {
        //         let mut kv: Client =
        //             Client::open("scanTestDB2".to_string(), 100, crate::StorageType::SST);

        //         let folder_path: &str = "./scanTestDB2/";
        //         create_dir_all(folder_path).expect("Create dir all has failed!");

        //         // put in tree
        //         for i in 0..50 {
        //             kv.put(i as i64, i * 10 as i64);
        //         }
        //         for i in 50..100 {
        //             kv.put(i, i * 2);
        //         }
        //         for i in 25..50 {
        //             kv.put(i, i);
        //         }

        //         let output_lst = kv.scan(0, 125); // 0-25 is i * 10, 25-50 is i, 50-100 is i*2

        //         // check!
        //         for i in 0..25 {
        //             assert!(output_lst[i] == (i as i64, i as i64 * 10));
        //         }
        //         for i in 25..50 {
        //             assert!(output_lst[i] == (i as i64, i as i64));
        //         }
        //         for i in 50..100 {
        //             assert!(output_lst[i] == (i as i64, i as i64 * 2));
        //         }

        //         for i in 0..kv.sst_num {
        //             remove_file(format!("{}output_{}.bin", folder_path, i))
        //                 .expect("Remove file has failed!");
        //         }
        //         remove_dir(folder_path).expect("Remove dir has failed!");
        //     }
        // }
    }

    mod btree {
        mod get_value {
            use crate::{Client, KVConfig};
            use std::fs::create_dir_all;
            use std::time::Instant;

            #[test] // 20k
            fn test_get_from_ssts_medium() {
                let mut kv: Client = Client::open(
                    "BTree_getTestDB1".to_string(),
                    KVConfig::default()
                        .storage_type(crate::StorageType::BTree)
                        .cleanup(true),
                );

                let folder_path: &str = "./BTree_getTestDB1/";
                create_dir_all(folder_path).expect("Create dir all has failed!");

                for i in 0..20005 {
                    kv.put(i as i64, (i * 2) as i64);
                }

                let key1: i64 = 3899;
                let key2: i64 = 8763;
                let key3: i64 = 20006;
                let start_time = Instant::now();
                assert_eq!(Some(3899 * 2), kv.get(key1));
                let end_time = Instant::now();
                // Calculate the time difference
                let elapsed_time = end_time.duration_since(start_time);
                // Print the time difference
                println!("Elapsed time: {:?}", elapsed_time);

                assert_eq!(Some(8763 * 2), kv.get(key2));
                assert_eq!(None, kv.get(key3));
            }

            // #[test] // 200k, commented takes long
            // fn test_get_from_ssts_large() {
            //     let mut kv: KV = KV::open("BTree_getTestDB2".to_string(), 200000, 1);

            //     let folder_path: &str = "./BTree_getTestDB2/";
            //     create_dir_all(folder_path).expect("Create dir all has failed!");
            //     for i in 0..200005 {
            //         kv.put(i as i64, (i * 2) as i64);
            //     }

            //     let key1: i64 = 38990;
            //     let key2: i64 = 8763;
            //     let key3: i64 = 20006;
            //     let start_time = Instant::now();
            //     assert_eq!(Some(38990 * 2), kv.get(key1));
            //     let end_time = Instant::now();
            //     // Calculate the time difference
            //     let elapsed_time = end_time.duration_since(start_time);
            //     // Print the time difference
            //     println!("Elapsed time: {:?}", elapsed_time);

            //     // assert_eq!(Some(8763 * 2), kv.get(key2));
            //     // assert_eq!(None, kv.get(key3));

            //     for i in 0..kv.sst_num {
            //         remove_file(format!("{}output_{}.bin", folder_path, i))
            //             .expect("Remove file has failed!");
            //     }
            //     remove_dir(folder_path).expect("Remove dir has failed!");
            // }
        }
        // mod scan {
        //     use crate::Client;
        //     use std::fs::{create_dir_all, remove_dir, remove_file};

        //     #[test]
        //     fn test_scan_from_tree_and_sst() {
        //         let mut kv: Client = Client::open(
        //             "BTree_scanTestDB1".to_string(),
        //             20000,
        //             crate::StorageType::BTree,
        //         );

        //         let folder_path: &str = "./BTree_scanTestDB1/";
        //         create_dir_all(folder_path).expect("Create dir all has failed!");

        //         for i in 0..20009 {
        //             kv.put(i as i64, i * 2 as i64);
        //         }

        //         let output_lst = kv.scan(13985, 19697);

        //         let mut j: i64 = 13985;
        //         for kv in output_lst.iter() {
        //             assert_eq!(*kv, (j, j * 2));
        //             j += 1;
        //         }
        //         assert!(j == 19698);

        //         for i in 0..kv.sst_num {
        //             remove_file(format!("{}output_{}.bin", folder_path, i))
        //                 .expect("Remove file has failed!");
        //         }
        //         remove_dir(folder_path).expect("Remove dir has failed!");
        //     }

        //     #[test]
        //     fn test_scan_from_tree_and_sst_large() {
        //         // damn my mac cpu fanning vol +++++
        //         let mut kv: KV = KV::open("BTree_scanTestDB2".to_string(), 50, 0);

        //         let folder_path: &str = "./BTree_scanTestDB2/";
        //         create_dir_all(folder_path).expect("Create dir all has failed!");

        //         for i in 0..10000007 {
        //             kv.put(i as i64, i * 2 as i64);
        //         }

        //         let output_lst = kv.scan(9095, 300007);

        //         let mut j: i64 = 9095;
        //         for kv in output_lst.iter() {
        //             assert_eq!(*kv, (j, j * 2));
        //             j += 1;
        //         }
        //         assert!(j == 300008);

        //         for i in 0..kv.sst_num {
        //             remove_file(format!("{}output_{}.bin", folder_path, i))
        //                 .expect("Remove file has failed!");
        //         }
        //         remove_dir(folder_path).expect("Remove dir has failed!");
        //     }
        // }
    }
}
