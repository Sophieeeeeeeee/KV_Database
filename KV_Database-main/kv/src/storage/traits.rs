use std::collections::HashMap;

/// Trait to generalize the work of different storage methods.
pub trait DiskStorage {
    /// Function to fetch the value at a particular `key` if it exists.
    /// # Arguments
    /// * `self` - A mutable ref to `DiskStorage` to search.
    /// * `key` - The key who's value is being searched.
    fn get(&mut self, key: i64) -> Option<i64>;
    /// Function to fetch the values at a particular key range if they exists. From `start` to `end` INCLUSIVE.
    /// # Arguments
    /// * `self` - A mutable ref to `DiskStorage` to search.
    /// * `start` - The begining of the scan range (INCLUSIVE).
    /// * `end` - The end of the scan range (INCLUSIVE).
    /// * `kv_hash` - The HashMap to store the output so we do not have duplicates.
    fn scan(&mut self, start: i64, end: i64, hash: &mut HashMap<i64, i64>);
    /// Function to flush the current `Memtable` contents into an SST.
    /// # Arguments
    /// * `self` - A mutable ref to the `DiskStorage` to flush.
    /// * `sst_count` - The number of SSTs already in the DB.
    /// * `contents` - The contents that need to be flushed.
    fn flush(&mut self, sst_count: u32, contents: Vec<(i64, i64)>);
}
