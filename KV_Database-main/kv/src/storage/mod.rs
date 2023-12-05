mod btree;
mod lsm;
mod part3btree;
mod traits;

pub use lsm::LSMTree;

use std::collections::HashMap;
pub use traits::DiskStorage;

use crate::{
    buffer::BufferPool,
    serde::{get_value_ssts, scan_ssts, serialize_kv_to_file},
};

use self::btree::{
    convert_sorted_arr_to_b_tree_arr_and_serialize, get_b_tree_ssts, scan_b_tree_ssts,
};

/// Struct of the `AppendOnlyLog` storage type.
pub struct AppendOnlyLog {
    name: String,
}

// Implementation of the `AppendOnlyLog` storage type.
impl AppendOnlyLog {
    /// Creating a new `AppendOnlyLog` given the `name`.
    /// # Arguments
    /// * `name` - The name of the newly created `AppendOnlyLog`.
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

// The implementation of the `AppendOnlyLog` as a `DiskStorage` type. Function docs in "traits.rs".
impl DiskStorage for AppendOnlyLog {
    fn get(&mut self, key: i64) -> Option<i64> {
        get_value_ssts(&self.name, key)
    }

    fn scan(&mut self, start: i64, end: i64, hash: &mut HashMap<i64, i64>) {
        scan_ssts(&self.name, start, end, hash);
    }

    fn flush(&mut self, sst_count: u32, contents: Vec<(i64, i64)>) {
        let file_path = format!("{}/output_{}.bin", self.name, sst_count);
        serialize_kv_to_file(&file_path, &contents)
    }
}

/// Struct of the `BTree` storage type.
pub struct BTree {
    name: String,
    pool: BufferPool,
}

// Implementation of the `BTree` storage type.
impl BTree {
    /// Creating a new `BTree` given the `name` and a `buffer_pool_size`.
    /// # Arguments
    /// * `name` - The name of the newly created `BTree`.
    /// * `buffer_pool_size` - The size of the buffer pool.
    pub fn new(name: String, buffer_pool_size: usize) -> Self {
        Self {
            name,
            pool: BufferPool::new(buffer_pool_size),
        }
    }
}

// The implementation of the `BTree` as a `DiskStorage` type. Function docs in "traits.rs".
impl DiskStorage for BTree {
    fn get(&mut self, key: i64) -> Option<i64> {
        get_b_tree_ssts(&self.name, key, &mut self.pool)
    }

    fn scan(&mut self, start: i64, end: i64, hash: &mut HashMap<i64, i64>) {
        scan_b_tree_ssts(&self.name, start, end, hash, &mut self.pool);
    }

    fn flush(&mut self, sst_count: u32, contents: Vec<(i64, i64)>) {
        let file_path = format!("{}/output_{}.bin", self.name, sst_count);
        convert_sorted_arr_to_b_tree_arr_and_serialize(&file_path, &contents)
    }
}
