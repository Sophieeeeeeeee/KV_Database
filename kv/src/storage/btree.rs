use std::collections::HashMap;
use std::fs::metadata;

use crate::{
    buffer::BufferPool,
    serde::{binary_search_array_start_index, get_sst_names, serialize_kv_to_file, PAGE_SIZE},
};

/// The number of entries in a given page (PAGE_SIZE / 16).
const ENTRIES: usize = 256;

/*
    The following functions are helper function.
*/

/// Helper function to flush the `Memtable` into a `BTree` implementation SST.
/// # Arguments
/// * `file_path` - The path to the new SST.
/// * `leaf_lst` - The list of nodes to serialize (content leaf nodes).
pub fn convert_sorted_arr_to_b_tree_arr_and_serialize(file_path: &str, leaf_lst: &Vec<(i64, i64)>) {
    let mut num_ptrs: usize = (leaf_lst.len() + (ENTRIES - 1)) / ENTRIES; // ceil

    // special handling: first internal nodes layer
    let mut candidates: Vec<i64> = (0..leaf_lst.len())
        .step_by(ENTRIES) // ENTRIES per page
        .map(|i| leaf_lst[i].0)
        .skip(1)
        .collect();

    // [i64] = node of one layer, [node1, node2] = one internal layer, [layer1, layer2] = tree
    let mut internal_levels: Vec<Vec<Vec<i64>>> = Vec::new();
    while !candidates.is_empty() {
        // construct internal layers
        let curr_level_num_nodes: usize = (num_ptrs + ENTRIES - 1) / ENTRIES; // ceil

        let keys_per_node: usize = (num_ptrs - (2 * curr_level_num_nodes)) / curr_level_num_nodes;
        // internal node with idx < excess_keys get an extra key
        let excess_keys: usize = (num_ptrs - (2 * curr_level_num_nodes)) % curr_level_num_nodes;

        let mut curr_node_idx_in_layer: usize = 0;
        let mut i: usize = 0;
        let mut next_layer_candidates: Vec<i64> = Vec::new();
        let mut internal_level: Vec<Vec<i64>> = Vec::new();

        while i < candidates.len() {
            // construct internal layer
            let mut j: usize = 0;
            let n_keys: usize =
                1 + keys_per_node + ((curr_node_idx_in_layer < excess_keys) as usize);
            let mut curr_node: Vec<i64> = Vec::with_capacity(n_keys);
            while j < n_keys {
                // construct internal node
                curr_node.push(candidates[i]);
                i += 1;
                j += 1;
            }

            internal_level.push(curr_node.clone());

            if i < candidates.len() {
                next_layer_candidates.push(candidates[i]);
                i += 1;
            }

            curr_node_idx_in_layer += 1;
        }
        internal_levels.push(internal_level.clone());

        num_ptrs = internal_level.len();
        candidates = next_layer_candidates;
    }

    // construct internal pages arr
    let mut pages_in_front: usize = 0;
    for internal_level in internal_levels.iter().rev() {
        let curr_level_num_nodes: usize = internal_level.len();
        let mut num_offset_pages: usize = pages_in_front + curr_level_num_nodes;

        for node in internal_level {
            let mut node_page_arr: Vec<(i64, i64)> = Vec::new();
            node_page_arr.push((node[0], num_offset_pages as i64));
            num_offset_pages += 1;

            for k in node {
                node_page_arr.push((*k, num_offset_pages as i64));
                num_offset_pages += 1;
            }

            serialize_kv_to_file(file_path, &node_page_arr);
            pages_in_front += 1;
        }
    }

    serialize_kv_to_file(file_path, leaf_lst);
}

/// Given a vector of KV pairs `kv_arr` and a `key`. Return the index of the smallest element >= to `key`.
/// # Arguments
/// * `kv_arr` - The array of KV pairs.
/// * `key` - The key in question.
pub fn binary_search_internal_se_key(arr: &Vec<(i64, i64)>, key: i64) -> Option<usize> {
    let mut left: usize = 1_usize;
    let mut right: usize = arr.len() - 1;
    let mut found_arr_idx: Option<usize> = None;

    while left <= right {
        let mid: usize = left + (right - left) / 2;

        match arr[mid].0.cmp(&key) {
            std::cmp::Ordering::Equal => {
                found_arr_idx = Some(mid);
                break;
            }
            std::cmp::Ordering::Less => {
                found_arr_idx = Some(mid);
                if mid == left {
                    break;
                }
                left = mid + 1;
            }
            std::cmp::Ordering::Greater => {
                right = mid - 1;
            }
        }
    }

    found_arr_idx
}

/*
    The following functions are specifically for the GET call to SSTs.
*/

/// Given the `filename`, `key`, and `buffer`, find and return the value of `key` if it exists.
/// # Arguments
/// * `filename` - The name of the SST being searched.
/// * `key` - The key who's value is being searched.
/// * `buffer` - The `BufferPool` to also search for the key.
fn search_b_tree_sst(filename: &str, key: i64, buffer: &mut BufferPool) -> Option<i64> {
    let mut page_idx: usize = 0;
    let value: Option<i64>;

    loop {
        let arr: Vec<(i64, i64)> = buffer.find_page(filename, page_idx * PAGE_SIZE);

        if arr.len() > 1 && arr[0].0 == arr[1].0 {
            // case internal node page
            let arr_idx: usize = binary_search_internal_se_key(&arr, key).unwrap_or(0_usize);
            assert!(arr[arr_idx].1 >= 0);
            page_idx = arr[arr_idx].1 as usize;
        } else {
            // case leaf page
            value = binary_search_array_start_index(&arr, key).and_then(|i| {
                if arr[i].0 == key {
                    Some(arr[i].1)
                } else {
                    None
                }
            });
            break;
        }
    }
    value
}

/// Given the `db_name`, `key`, and `buffer`, find and return the value of `key` if it exists accross all SSTs in DB.
/// # Arguments
/// * `db_name` - The name of the DB being searched.
/// * `key` - The key who's value is being searched.
/// * `buffer` - The `BufferPool` to also search for the key.
pub fn get_b_tree_ssts(db_name: &str, key: i64, buffer: &mut BufferPool) -> Option<i64> {
    let sst_names: Vec<String> = get_sst_names(db_name);

    let mut value: Option<i64> = None;
    for name in sst_names {
        value = search_b_tree_sst(&name, key, buffer);
        if value.is_some() {
            break;
        }
    }

    value
}

/*
    The following functions are specifically for the SCAN call to SSTs.
*/

/// Given a `file_path`, keep adding values to the `kv_hash` result structure until the scan range is exit
/// or the end of SST is reached.
/// # Arguments
/// * `file_path` - The path to the SST in question.
/// * `total_pages` - The number of pages in the SST.
/// * `page_idx` - The index of the page to scan.
/// * `arr_idx` - The index of where to start the scan in the page.
/// * `end` - The end of the scan range.
/// * `kv_hash` - The HashMap to store the results.
/// * `buffer` - The `BufferPool` to also search for the keys.
pub fn scan_b_tree_file(
    file_path: &str,
    total_pages: usize,
    page_idx: usize,
    arr_idx: usize,
    end: i64,
    kv_hash: &mut HashMap<i64, i64>,
    buffer: &mut BufferPool,
) {
    let mut local_page_idx = page_idx;
    let mut local_arr_idx = arr_idx;

    while local_page_idx < total_pages {
        let kv_arr: Vec<(i64, i64)> = buffer.find_page(file_path, local_page_idx * PAGE_SIZE);

        let mut i = local_arr_idx;
        while i < kv_arr.len() && kv_arr[i].0 <= end {
            kv_hash.entry(kv_arr[i].0).or_insert(kv_arr[i].1);
            i += 1;
        }

        let save_len_before_ownership = kv_arr.len();

        if i != save_len_before_ownership {
            break;
        }

        local_page_idx += 1;
        local_arr_idx = 0;
    }
}

/// Given a `file_path` to an SST, get it ready to be scanned by finding the start index of the scan
/// and then call the scan_b_tree_file when the location was found to populate the `kv_hash`.
/// # Arguments
/// * `file_path` - The path to the SST in question.
/// * `total_pages` - The number of pages in the SST.
/// * `page_idx` - The index of the page to scan.
/// * `arr_idx` - The index of where to start the scan in the page.
/// * `end` - The end of the scan range.
/// * `kv_hash` - The HashMap to store the results.
/// * `buffer` - The `BufferPool` to also search for the keys.
fn scan_b_tree_sst(
    file_path: &str,
    start: i64,
    end: i64,
    kv_hash: &mut HashMap<i64, i64>,
    total_pages: usize,
    buffer: &mut BufferPool,
) {
    let mut page_idx: usize = 0;
    let start_page_idx: usize;

    // find starting point in file
    loop {
        let arr: Vec<(i64, i64)> = buffer.find_page(file_path, page_idx * PAGE_SIZE);

        if arr.len() > 1 && arr[0].0 == arr[1].0 {
            // case internal node page
            let arr_idx: usize = binary_search_internal_se_key(&arr, start).unwrap_or(0_usize);
            assert!(arr[arr_idx].1 >= 0);
            page_idx = arr[arr_idx].1 as usize;
        } else {
            // case leaf page
            start_page_idx = page_idx;
            if let Some(start_arr_idx) = binary_search_array_start_index(&arr, start) {
                scan_b_tree_file(
                    file_path,
                    total_pages,
                    start_page_idx,
                    start_arr_idx,
                    end,
                    kv_hash,
                    buffer,
                );
            }
            break;
        }
    }
}

/// This is the primary call from the Client code to scan through the SSTs in the DB `db_name` to find the values
/// from `start` to `end` (both INCLUSIVE). It stores its findings in `kv_hash` as to eliminate any duplicates.
/// # Arguments
/// * `db_name` - The name of the database to search.
/// * `start` - The start key range of the scan.
/// * `end` - The end key range of the scan.
/// * `kv_hash` - The HashMap to store the results.
/// * `buffer` - The `BufferPool` to also search for the keys.
pub fn scan_b_tree_ssts(
    db_name: &str,
    start: i64,
    end: i64,
    kv_hash: &mut HashMap<i64, i64>,
    buffer: &mut BufferPool,
) {
    let num_elements_in_range: usize = (end - start) as usize;

    let sst_names: Vec<String> = get_sst_names(db_name);
    for name in sst_names {
        let total_pages: usize =
            (metadata(&name).expect("Metadata call failed!").len() as usize) / PAGE_SIZE;

        scan_b_tree_sst(&name, start, end, kv_hash, total_pages, buffer);

        if kv_hash.len() == num_elements_in_range {
            break;
        }
    }
}
