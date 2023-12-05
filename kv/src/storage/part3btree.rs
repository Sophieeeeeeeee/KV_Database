#![allow(dead_code)]

use crate::serde::{binary_search_array_start_index, deserialize_page, serialize_kv_to_file};
use crate::storage::btree::{binary_search_internal_se_key, scan_b_tree_file};
use crate::storage::BufferPool;
use std::collections::HashMap;
use std::fs::metadata;

const PAGE_SIZE: usize = 4096;
const ENTRIES: usize = 256;

pub fn part3_create_b_tree_internal_file(leaf_file_path: &str, internal_file_path: &str) {
    let total_pages: usize = (metadata(leaf_file_path)
        .expect("Metadata call failed!")
        .len() as usize)
        / PAGE_SIZE;
    let mut num_ptrs = total_pages;

    // special handling: first internal nodes layer
    let mut candidates: Vec<i64> = Vec::new();
    for i in 1..total_pages {
        let kv_arr: Vec<(i64, i64)> = deserialize_page(leaf_file_path, i * PAGE_SIZE);
        candidates.push(kv_arr[0].0);
    }

    // [i64] = node of one layer, [node1, node2] = one internal layer, [layer1, layer2] = tree
    let mut internal_levels: Vec<Vec<Vec<i64>>> = Vec::new();
    while !candidates.is_empty() {
        // construct internal layers
        let curr_level_num_nodes = (num_ptrs + ENTRIES - 1) / ENTRIES; // ceil

        let keys_per_node = (num_ptrs - (2 * curr_level_num_nodes)) / curr_level_num_nodes;
        // internal node with idx < excess_keys get an extra key
        let excess_keys = (num_ptrs - (2 * curr_level_num_nodes)) % curr_level_num_nodes;

        let mut curr_node_idx_in_layer = 0;
        let mut i = 0;
        let mut next_layer_candidates: Vec<i64> = Vec::new();
        let mut internal_level: Vec<Vec<i64>> = Vec::new();

        while i < candidates.len() {
            // construct internal layer
            let mut j = 0;
            let n_keys = 1 + keys_per_node + ((curr_node_idx_in_layer < excess_keys) as usize);
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
        num_ptrs = internal_level.len();
        internal_levels.push(internal_level);
        candidates = next_layer_candidates;
    }

    // construct internal pages arr
    let mut pages_in_front = 0;
    for internal_level in internal_levels.iter().rev() {
        let curr_level_num_nodes = internal_level.len();
        let mut num_offset_pages = pages_in_front + curr_level_num_nodes;

        for node in internal_level {
            let mut node_page_arr: Vec<(i64, i64)> = Vec::new();
            node_page_arr.push((node[0], num_offset_pages as i64));
            num_offset_pages += 1;

            for k in node {
                node_page_arr.push((*k, num_offset_pages as i64));
                num_offset_pages += 1;
            }

            serialize_kv_to_file(internal_file_path, &node_page_arr);
            pages_in_front += 1;
        }
    }
}

/////// get

pub fn part3_search_b_tree_sst(
    leaf_filename: &str,
    internal_filename: &str,
    key: i64,
    buffer: &mut BufferPool,
) -> Option<i64> {
    let internal_total_pages: usize = (metadata(internal_filename)
        .expect("Metadata call failed!")
        .len() as usize)
        / PAGE_SIZE;
    let mut page_idx: usize = 0;

    // internal file search
    loop {
        if page_idx >= internal_total_pages {
            break;
        }

        let arr: Vec<(i64, i64)> = buffer.find_page(internal_filename, page_idx * PAGE_SIZE);

        assert!(arr.len() > 1 && arr[0].0 == arr[1].0);
        let arr_idx = binary_search_internal_se_key(&arr, key).unwrap_or(0_usize);
        assert!(arr[arr_idx].1 >= 0);
        page_idx = arr[arr_idx].1 as usize;
    }

    // leaf file search
    page_idx -= internal_total_pages; // TODO: if filter, + btree_idx return from deserialize_filter
    let kv_arr: Vec<(i64, i64)> = buffer.find_page(leaf_filename, page_idx * PAGE_SIZE);
    let value: Option<i64> = binary_search_array_start_index(&kv_arr, key).and_then(|i| {
        if kv_arr[i].0 == key {
            Some(kv_arr[i].1)
        } else {
            None
        }
    });

    value
}

/////// scan

pub fn part3_scan_b_tree_sst(
    leaf_filename: &str,
    internal_filename: &str,
    key1: i64,
    key2: i64,
    kv_hash: &mut HashMap<i64, i64>,
    buffer: &mut BufferPool,
) {
    let internal_total_pages: usize = (metadata(internal_filename)
        .expect("Metadata call failed!")
        .len() as usize)
        / PAGE_SIZE;
    let mut page_idx: usize = 0;

    // internal file search
    loop {
        if page_idx >= internal_total_pages {
            break;
        }

        let arr: Vec<(i64, i64)> = buffer.find_page(internal_filename, page_idx * PAGE_SIZE);
        assert!(arr.len() > 1 && arr[0].0 == arr[1].0);
        let arr_idx = binary_search_internal_se_key(&arr, key1).unwrap_or(0_usize);
        assert!(arr[arr_idx].1 >= 0);
        page_idx = arr[arr_idx].1 as usize;
    }

    // leaf file search
    let leaf_total_pages: usize = (metadata(leaf_filename)
        .expect("Metadata call failed!")
        .len() as usize)
        / PAGE_SIZE;

    let start_page_idx = page_idx - internal_total_pages; // TODO: if filter, + btree_idx return from deserialize_filter
    let kv_arr: Vec<(i64, i64)> = buffer.find_page(leaf_filename, start_page_idx * PAGE_SIZE);
    if let Some(start_arr_idx) = binary_search_array_start_index(&kv_arr, key1) {
        scan_b_tree_file(
            leaf_filename,
            leaf_total_pages,
            start_page_idx,
            start_arr_idx,
            key2,
            kv_hash,
            buffer,
        );
    }
}

/////

#[cfg(test)]
mod tests {

    use crate::storage::part3btree::{
        part3_create_b_tree_internal_file, part3_scan_b_tree_sst, part3_search_b_tree_sst,
    };
    use crate::storage::serialize_kv_to_file;
    use crate::storage::BufferPool;

    use std::collections::HashMap;
    use std::fs::{create_dir_all, remove_dir, remove_file};

    #[test]
    fn test_create_b_tree_internal_file() {
        let folder_path: &str = "./Part3BTree_DB1";
        let leaf_filename: String = format!("{}output_1_leaf.bin", folder_path);
        let internal_filename: String = format!("{}output_1_internal.bin", folder_path);
        create_dir_all(folder_path).expect("Create dir all has failed!");

        let mut kv_arr: Vec<(i64, i64)> = Vec::new();
        for i in 0..256 * 100 {
            kv_arr.push((i as i64, (i * 2) as i64));
        }
        serialize_kv_to_file(&leaf_filename, &kv_arr);

        part3_create_b_tree_internal_file(&leaf_filename, &internal_filename);

        let mut buffer = BufferPool::new(16);
        // test get
        assert_eq!(
            Some(22679 * 2 as i64),
            part3_search_b_tree_sst(&leaf_filename, &internal_filename, 22679, &mut buffer)
        );
        assert_eq!(
            None,
            part3_search_b_tree_sst(&leaf_filename, &internal_filename, 256 * 100, &mut buffer)
        );
        // test scan
        let mut kv_hash: HashMap<i64, i64> = HashMap::new();
        part3_scan_b_tree_sst(
            &leaf_filename,
            &internal_filename,
            333 as i64,
            9999 as i64,
            &mut kv_hash,
            &mut buffer,
        );
        for i in 333..9999 {
            assert_eq!(i * 2 as i64, *kv_hash.get(&i).unwrap());
        }

        remove_file(&leaf_filename).expect("Remove file has failed!");
        remove_file(&internal_filename).expect("Remove file has failed!");
        remove_dir(folder_path).expect("Remove dir has failed!");
    }
}
