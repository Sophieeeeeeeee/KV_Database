use std::{
    collections::HashMap,
    fs::{create_dir, remove_file, File, OpenOptions},
    io::Write,
};

use crate::{
    buffer::BufferPool,
    filter::{Bitmap, BloomFilter},
    serde::{pad_page_bytes, serialize_kv_to_file, PAGE_SIZE},
    storage::part3btree,
};

use super::DiskStorage;

pub struct LSMTree {
    name: String,
    pool: BufferPool,
    tree_size: u32,
    filters: Vec<Option<Bitmap>>,
    memtable_size: u32,
}

impl LSMTree {
    pub fn new(name: String, buffer_pool_size: usize, memtable_size: u32) -> Self {
        create_dir(&name).unwrap();
        let mut filters = vec![];
        for _ in 0..=50 {
            filters.push(None);
        }
        Self {
            name,
            pool: BufferPool::new(buffer_pool_size),
            tree_size: 0,
            filters,
            memtable_size,
        }
    }

    fn merge_ssts(&mut self, level: u32) {
        // ssts that we are merging
        let first_sst = format!(
            "{}/output_leaf_{}_{}.bin",
            self.name,
            level,
            self.tree_size - 2_u32.pow(level - 1)
        );
        let first_internal = format!(
            "{}/output_internal_{}_{}.bin",
            self.name,
            level,
            self.tree_size - 2_u32.pow(level - 1)
        );
        let second_sst = format!("{}/output_leaf_{}_{}.bin", self.name, level, self.tree_size);
        let second_internal = format!(
            "{}/output_internal_{}_{}.bin",
            self.name, level, self.tree_size
        );

        let first_page_count = File::open(&first_sst)
            .expect("SST1 not found")
            .metadata()
            .unwrap()
            .len()
            / PAGE_SIZE as u64;
        let second_page_count = File::open(&second_sst)
            .expect("SST2 not found")
            .metadata()
            .unwrap()
            .len()
            / PAGE_SIZE as u64;

        let mut first_page_idx: u64 = 0;
        let mut second_page_idx: u64 = 0;

        let mut first_buffer = self
            .pool
            .find_page(&first_sst, first_page_idx as usize * PAGE_SIZE);
        let mut second_buffer = self
            .pool
            .find_page(&second_sst, second_page_idx as usize * PAGE_SIZE);

        let mut output_buffer = Vec::with_capacity(256);

        let output_file_name = format!(
            "{}/output_leaf_{}_{}.bin",
            self.name,
            level + 1,
            self.tree_size
        );
        let output_file_internal = format!(
            "{}/output_internal_{}_{}.bin",
            self.name,
            level + 1,
            self.tree_size
        );

        let mut new_filter = Bitmap::new(2_u64.pow(level) * self.memtable_size as u64 * 10);

        let mut output_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&output_file_name)
            .expect("rip");

        // merge SSTs together
        loop {
            if first_buffer.is_empty() {
                first_page_idx += 1;
                if first_page_idx == first_page_count {
                    break;
                }
                first_buffer = self
                    .pool
                    .find_page(&first_sst, first_page_idx as usize * PAGE_SIZE);
            }
            if second_buffer.is_empty() {
                second_page_idx += 1;
                if second_page_idx == second_page_count {
                    break;
                }
                second_buffer = self
                    .pool
                    .find_page(&second_sst, second_page_idx as usize * PAGE_SIZE);
            }

            let first_element = first_buffer[0];
            let second_element = second_buffer[0];

            match &first_element.0.cmp(&second_element.0) {
                std::cmp::Ordering::Less => {
                    output_buffer.push(first_element);
                    first_buffer.drain(0..1);
                    new_filter.insert_key(first_element.0);
                }
                std::cmp::Ordering::Greater => {
                    output_buffer.push(second_element);
                    second_buffer.drain(0..1);
                    new_filter.insert_key(second_element.0);
                }
                std::cmp::Ordering::Equal => {
                    // keep newest key if duplicates
                    output_buffer.push(second_element);
                    first_buffer.drain(0..1);
                    second_buffer.drain(0..1);
                    new_filter.insert_key(second_element.0);
                }
            }

            // append to output file when buffer is full
            if output_buffer.len() == 256 {
                flush_output_buffer(&mut output_file, &mut output_buffer)
            }
        }

        // write remaining kv pairs to output buffer
        while first_page_idx < first_page_count {
            if first_buffer.is_empty() {
                first_page_idx += 1;
                if first_page_idx == first_page_count {
                    break;
                }
                first_buffer = self
                    .pool
                    .find_page(&first_sst, first_page_idx as usize * PAGE_SIZE);
            }
            output_buffer.push(first_buffer[0]);
            new_filter.insert_key(first_buffer[0].0);
            first_buffer.drain(0..1);

            if output_buffer.len() == 256 {
                flush_output_buffer(&mut output_file, &mut output_buffer);
            }
        }

        while second_page_idx < second_page_count {
            if second_buffer.is_empty() {
                second_page_idx += 1;
                if second_page_idx == second_page_count {
                    break;
                }
                second_buffer = self
                    .pool
                    .find_page(&second_sst, second_page_idx as usize * PAGE_SIZE);
            }
            output_buffer.push(second_buffer[0]);
            new_filter.insert_key(second_buffer[0].0);
            second_buffer.drain(0..1);

            if output_buffer.len() == 256 {
                flush_output_buffer(&mut output_file, &mut output_buffer);
            }
        }

        if !output_buffer.is_empty() {
            flush_output_buffer(&mut output_file, &mut output_buffer);
        }

        part3btree::part3_create_b_tree_internal_file(&output_file_name, &output_file_internal);
        remove_file(first_sst).unwrap();
        remove_file(second_sst).unwrap();
        remove_file(first_internal).unwrap();
        remove_file(second_internal).unwrap();

        self.filters[level as usize] = None;
        self.filters[level as usize + 1] = Some(new_filter);
    }
}

fn flush_output_buffer(file: &mut File, output_buffer: &mut Vec<(i64, i64)>) {
    let mut bytes: Vec<u8> = Vec::new();

    for (key, value) in &mut *output_buffer {
        bytes.extend_from_slice(&key.to_be_bytes());
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    pad_page_bytes(&mut bytes);

    file.write_all(&bytes).unwrap();
    output_buffer.clear();
}

impl DiskStorage for LSMTree {
    fn get(&mut self, key: i64) -> Option<i64> {
        if self.tree_size == 0 {
            return None;
        }
        for i in 1..=self.tree_size.ilog2() + 1 {
            if self.tree_size & (1 << (i - 1)) == 0
                || !self.filters[0].as_ref().map_or(true, |a| a.check_key(key))
            {
                continue;
            }
            let leaf_filename = format!(
                "{}/output_leaf_{}_{}.bin",
                self.name,
                i,
                (self.tree_size / 2_u32.pow(i - 1)) * 2_u32.pow(i - 1)
            );
            let internal_filename = format!(
                "{}/output_internal_{}_{}.bin",
                self.name,
                i,
                (self.tree_size / 2_u32.pow(i - 1)) * 2_u32.pow(i - 1)
            );
            if let Some(a) = part3btree::part3_search_b_tree_sst(
                &leaf_filename,
                &internal_filename,
                key,
                &mut self.pool,
            ) {
                return Some(a);
            }
        }
        None
    }

    fn scan(&mut self, start: i64, end: i64, hash: &mut HashMap<i64, i64>) {
        if self.tree_size == 0 {
            return;
        }
        for i in 1..=self.tree_size.ilog2() + 1 {
            if self.tree_size & (1 << (i - 1)) == 0 {
                continue;
            }
            let leaf_filename = format!(
                "{}/output_leaf_{}_{}.bin",
                self.name,
                i,
                (self.tree_size / 2_u32.pow(i - 1)) * 2_u32.pow(i - 1)
            );
            let internal_filename = format!(
                "{}/output_internal_{}_{}.bin",
                self.name,
                i,
                (self.tree_size / 2_u32.pow(i - 1)) * 2_u32.pow(i - 1)
            );
            part3btree::part3_scan_b_tree_sst(
                &leaf_filename,
                &internal_filename,
                start,
                end,
                hash,
                &mut self.pool,
            );
        }
    }

    fn flush(&mut self, _: u32, contents: Vec<(i64, i64)>) {
        if contents.is_empty() {
            return;
        }
        self.tree_size += 1;
        let leaf_file_path = format!("{}/output_leaf_{}_{}.bin", self.name, 1, self.tree_size);
        let internal_file_path =
            format!("{}/output_internal_{}_{}.bin", self.name, 1, self.tree_size);
        serialize_kv_to_file(&leaf_file_path, &contents);

        part3btree::part3_create_b_tree_internal_file(&leaf_file_path, &internal_file_path);

        // create filter
        let mut b = Bitmap::new(10 * contents.len() as u64);
        for i in contents {
            b.insert_key(i.0);
        }
        self.filters[1] = Some(b);

        // merge ssts if necessary
        let mut level = 1;
        while self.tree_size & (1 << (level - 1)) == 0 {
            self.merge_ssts(level);
            level += 1;
        }
    }
}
