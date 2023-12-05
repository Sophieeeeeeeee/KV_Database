use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{create_dir_all, metadata, read_dir, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::slice::ChunksExact;

pub const PAGE_SIZE: usize = 4096;
const O_DIRECT: libc::c_int = 0x4000;

/*
    The following functions are for the serialization and deserialization processes.
    The private functions are helpers that should not be used elsewhere.
*/

/// Given `bytes` where the length is a multiple of 16 (a KV pair is 16 bytes),
/// mutate it and pad its length until it reaches the nearest `PAGE_SIZE` multiple.
/// # Arguments
/// * `bytes` - Vector with length multiple 16 of serialized KV pairs.
pub fn pad_page_bytes(bytes: &mut Vec<u8>) {
    let mut padding_size: usize = 0;
    if bytes.len() % PAGE_SIZE != 0 {
        padding_size = PAGE_SIZE - (bytes.len() % PAGE_SIZE);
    }
    assert!(padding_size % 16 == 0);

    while padding_size > 0 {
        let padding: [u8; 16] = [
            0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad,
            0xbe, 0xef,
        ];

        bytes.extend_from_slice(&padding[..16]);
        padding_size -= 16;
    }
    assert!(bytes.len() % 4096 == 0);
}

/// Given `file_path` and `page_offset`, deserialize the data at the location in the file and return the vector of KV pairs.
/// # Arguments
/// * `file_path` - The path to the file.
/// * `page_offset` - The offset to the wanted page in the file.
pub fn deserialize_page(file_path: &str, page_offset: usize) -> Vec<(i64, i64)> {
    let mut file: File = OpenOptions::new()
        .read(true)
        .custom_flags(O_DIRECT) // libc::O_DIRECT
        .open(file_path)
        .expect("Deserializer: open file failed!");

    file.seek(SeekFrom::Start(page_offset as u64))
        .expect("Deserializer: file seek failed!");

    let mut bytes: Vec<u8> = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut bytes)
        .expect("Deserializer: file exact read failed!");

    let padding: [u8; 16] = [
        0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe,
        0xef,
    ];
    let mut non_padding_idx: usize = bytes.len();
    while non_padding_idx >= 16 && bytes[non_padding_idx - 16..non_padding_idx] == padding {
        non_padding_idx -= 16;
    }

    let bytes_without_padding: &[u8] = &bytes[..non_padding_idx];
    let iter: ChunksExact<'_, u8> = bytes_without_padding.chunks_exact(16);

    let kv_arr: Vec<(i64, i64)> = iter
        .map(|chunk| {
            // Convert the first 8 bytes to i64 for key
            let key_bytes: &[u8] = &chunk[..8];
            let key: i64 = i64::from_be_bytes(
                key_bytes
                    .try_into()
                    .expect("Deserializer: invalid key chunk size!"),
            );

            // Fetch the next chunk (8 bytes) for the value
            let value_bytes: &[u8] = &chunk[8..];
            let value: i64 = i64::from_be_bytes(
                value_bytes
                    .try_into()
                    .expect("Deserializer: invalid value chunk size!"),
            );

            // Return the key-value pair
            (key, value)
        })
        .collect();

    kv_arr
}

/// Given `file_path` and `kv_arr`, serialize the `kv_arr` vector and store it in the sst at `file_path`.
/// # Arguments
/// * `file_path` - The path to the file.
/// * `kv_arr` - The vector of KV pairs.
pub fn serialize_kv_to_file(file_path: &str, kv_arr: &Vec<(i64, i64)>) {
    let mut bytes: Vec<u8> = Vec::new();

    for (key, value) in kv_arr {
        bytes.extend_from_slice(&key.to_be_bytes());
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    pad_page_bytes(&mut bytes);

    // Create directories if they don't exist
    if let Some(parent_dir) = std::path::Path::new(&file_path).parent() {
        create_dir_all(parent_dir).expect("Serializer: file dir not found + failed to create!");
    }

    let mut file: File = OpenOptions::new()
        .create(true)
        .append(true)
        .custom_flags(O_DIRECT) // libc::O_DIRECT
        .open(file_path)
        .expect("Serializer: failed to create / append if file exists!");

    file.write_all(&bytes)
        .expect("Serializer: file write failed!");
}

/*
    The following functions are for the binary search processes from Part 1.
    The private functions are helpers that should not be used elsewhere.
    Note that some of these functions are also used in the "storage/btree.rs" file.
*/

/// Given `db_name`, output all the names of SSTs inside.
/// # Arguments
/// * `db_name` - The path to the database in question.
pub fn get_sst_names(db_name: &str) -> Vec<String> {
    let db_path: String = format!("./{}/", db_name);

    let mut sst_names: Vec<String> = vec![];
    if let Ok(entries) = read_dir(&db_path) {
        let num_sst: usize = entries.count();

        sst_names = (0..num_sst)
            .rev()
            .map(|i: usize| format!("{}output_{}.bin", db_path, i))
            .collect();
    }
    sst_names
}

/*
    The following functions are specifically for the GET call to SSTs.
*/

/// Given a vector of KV pairs `kv_arr` and a `key`, find the value associated with `key` if it is there.
/// # Arguments
/// * `kv_arr` - The vector of KV pairs.
/// * `key` - The key who's value we want.
fn binary_search_array(kv_arr: &Vec<(i64, i64)>, key: i64) -> Option<i64> {
    let mut left: usize = 0;
    let mut right: usize = kv_arr.len() - 1;

    while left <= right {
        let mid: usize = left + (right - left) / 2;

        match kv_arr[mid].0.cmp(&key) {
            Ordering::Equal => return Some(kv_arr[mid].1),
            Ordering::Greater => right = mid - 1,
            Ordering::Less => left = mid + 1,
        }
    }
    None
}

/// Given `file_path`, `total_pages`, and a `key`. Find the value of the `key` in the page at `file_path`.
/// # Arguments
/// * `file_path` - The path to the SST file in question.
/// * `total_pages` - The size of `file_path` in number of pages.
/// * `key` - The key who's value to find.
pub fn binary_search_file(file_path: &str, total_pages: usize, key: i64) -> Option<i64> {
    let mut left: usize = 0;
    let mut right: usize = total_pages - 1;

    while left <= right {
        let mid: usize = left + (right - left) / 2;

        let kv_arr: Vec<(i64, i64)> = deserialize_page(file_path, mid * PAGE_SIZE);
        let first_key: i64 = kv_arr.first().unwrap().0;
        let last_key: i64 = kv_arr.last().unwrap().0;

        if first_key <= key && key <= last_key {
            return binary_search_array(&kv_arr, key);
        } else if first_key > key {
            if mid == 0 {
                return None;
            }
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    None
}

/// This is the primary call from the Client code to search through the SSTs in the DB `db_name` to find the value of `key`.
/// # Arguments
/// * `db_name` - The name of the database to search.
/// * `key` - The key who's value to find.
pub fn get_value_ssts(db_name: &str, key: i64) -> Option<i64> {
    let sst_names: Vec<String> = get_sst_names(db_name);

    for name in sst_names {
        let total_pages: usize =
            (metadata(&name).expect("Metadata call failed!").len() as usize) / PAGE_SIZE;
        let value: Option<i64> = binary_search_file(&name, total_pages, key);
        if value.is_some() {
            return value;
        }
    }
    None
}

/*
    The following functions are specifically for the SCAN call to SSTs.
*/

/// Given a vector of KV pairs `kv_arr` and a `key`. Return the index of the smallest element >= to `key`.
/// # Arguments
/// * `kv_arr` - The array of KV pairs.
/// * `key` - The key in question.
pub fn binary_search_array_start_index(kv_arr: &Vec<(i64, i64)>, key: i64) -> Option<usize> {
    let mut found_arr_idx: Option<usize> = None;

    let mut left: usize = 0;
    let mut right: usize = kv_arr.len() - 1;

    while left <= right {
        let mid: usize = left + (right - left) / 2;

        if kv_arr[mid].0 >= key {
            found_arr_idx = Some(mid);
            if mid == left {
                break;
            }
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    found_arr_idx
}

/// Given the `file_path`, `total_pages`, `start` key, and `end` key, return two indexes.
/// The first index should be for a page in the SST and the second index for a KV pair inside of the page
/// such that together they point to the first KV pair in the scan range inside of that particular SST.
/// # Arguments
/// * `file_path` - The path to the SST in question.
/// * `total_pages` - The number of pages in the SST.
/// * `start` - The start range of the scan.
/// * `end` - The end range of the scan.
pub fn binary_search_sst_start_index(
    file_path: &str,
    total_pages: &usize,
    start: i64,
    end: i64,
) -> (Option<usize>, Option<usize>) {
    let mut start_page_idx: Option<usize> = None;
    let mut start_arr_idx: Option<usize> = None;

    let first_page_arr: Vec<(i64, i64)> = deserialize_page(file_path, 0);
    let last_page_arr: Vec<(i64, i64)> = deserialize_page(file_path, (total_pages - 1) * PAGE_SIZE);

    if first_page_arr[0].0 <= start && start <= last_page_arr[last_page_arr.len() - 1].0 {
        // case start in sst
        let mut left: usize = 0;
        let mut right: usize = total_pages - 1;
        let mut kv_arr: Vec<(i64, i64)> = Vec::new();

        // find start_page_idx
        while left <= right {
            let mid: usize = left + (right - left) / 2;

            kv_arr = deserialize_page(file_path, mid * PAGE_SIZE);

            if kv_arr[0].0 <= start && start <= kv_arr[kv_arr.len() - 1].0 {
                start_page_idx = Some(mid);
                break;
            } else if start < kv_arr[0].0 {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        // find start_arr_idx
        start_arr_idx = binary_search_array_start_index(&kv_arr, start);
    } else if start < first_page_arr[0].0 && first_page_arr[0].0 <= end {
        start_page_idx = Some(0_usize);
        start_arr_idx = Some(0_usize);
    }

    (start_page_idx, start_arr_idx)
}

/// Given a `file_path`, keep adding values to the `kv_hash` result structure until the scan range is exit
/// or the end of SST is reached.
/// # Arguments
/// * `file_path` - The path to the SST in question.
/// * `total_pages` - The number of pages in the SST.
/// * `page_idx` - The index of the page to scan.
/// * `arr_idx` - The index of where to start the scan in the page.
/// * `end` - The end of the scan range.
/// * `kv_hash` - The HashMap to store the results.
pub fn scan_file(
    file_path: &str,
    total_pages: usize,
    mut page_idx: usize,
    mut arr_idx: usize,
    end: i64,
    kv_hash: &mut HashMap<i64, i64>,
) {
    while page_idx != total_pages {
        let kv_arr: Vec<(i64, i64)> = deserialize_page(file_path, page_idx * PAGE_SIZE);
        let kv_arr_len: usize = kv_arr.len();

        while arr_idx < kv_arr_len && kv_arr[arr_idx].0 <= end {
            kv_hash
                .entry(kv_arr[arr_idx].0)
                .or_insert(kv_arr[arr_idx].1);
            arr_idx += 1;
        }

        arr_idx = 0;
        page_idx += 1;
    }
}

/// This is the primary call from the Client code to scan through the SSTs in the DB `db_name` to find the values
/// from `start` to `end` (both INCLUSIVE). It stores its findings in `kv_hash` as to eliminate any duplicates.
/// # Arguments
/// * `db_name` - The name of the database to search.
/// * `start` - The start key range of the scan.
/// * `end` - The end key range of the scan.
/// * `kv_hash` - The HashMap to store the results.
pub fn scan_ssts(db_name: &str, start: i64, end: i64, kv_hash: &mut HashMap<i64, i64>) {
    let num_elements_in_range: usize = (end - start) as usize;

    let sst_names: Vec<String> = get_sst_names(db_name);
    for name in sst_names {
        let total_pages: usize =
            (metadata(&name).expect("Metadata call failed!").len() as usize) / PAGE_SIZE;

        if let (Some(page_idx), Some(arr_idx)) =
            binary_search_sst_start_index(&name, &total_pages, start, end)
        {
            scan_file(&name, total_pages, page_idx, arr_idx, end, kv_hash);
        }

        if kv_hash.len() == num_elements_in_range {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    mod serde {
        use crate::serde::{
            binary_search_array, binary_search_array_start_index, binary_search_file,
            binary_search_sst_start_index, deserialize_page, get_sst_names, get_value_ssts,
            pad_page_bytes, scan_file, scan_ssts, serialize_kv_to_file, PAGE_SIZE,
        };

        use std::{
            collections::HashMap,
            fs::{create_dir_all, metadata, remove_dir, remove_file, File},
        };

        #[test]
        fn test_pad_page_bytes() {
            let mut bytes: Vec<u8> = Vec::new();

            pad_page_bytes(&mut bytes);
            assert_eq!(bytes.len(), 0);

            for i in 0..16 {
                bytes.push(i);
            }

            pad_page_bytes(&mut bytes);
            assert_eq!(bytes.len(), PAGE_SIZE);

            for i in 0..16 {
                bytes.push(i);
            }

            pad_page_bytes(&mut bytes);
            assert_eq!(bytes.len(), 2 * PAGE_SIZE);
        }

        #[test]
        fn test_serialize_deserialize() {
            let folder_path: &str = "./serdeTestDB/";
            let file_path_string: String = format!("{}output_1.bin", folder_path);
            let file_path: &str = file_path_string.as_str();

            create_dir_all(folder_path).expect("Create dir all has failed!");

            let mut kv_vec: Vec<(i64, i64)> = Vec::new();
            let mut kv_expected1: Vec<(i64, i64)> = Vec::new();
            let mut kv_expected2: Vec<(i64, i64)> = Vec::new();
            let mut kv_expected3: Vec<(i64, i64)> = Vec::new();
            for i in 0..((PAGE_SIZE / 16) * 3) as i64 {
                kv_vec.push((i, i * 2));
                if i < (PAGE_SIZE / 16) as i64 {
                    kv_expected1.push((i, i * 2));
                } else if i < ((PAGE_SIZE / 16) * 2) as i64 {
                    kv_expected2.push((i, i * 2));
                } else {
                    kv_expected3.push((i, i * 2));
                }
            }
            serialize_kv_to_file(file_path, &kv_vec);

            assert_eq!(kv_expected1, deserialize_page(file_path, 0));
            assert_eq!(kv_expected2, deserialize_page(file_path, PAGE_SIZE));
            assert_eq!(kv_expected3, deserialize_page(file_path, PAGE_SIZE * 2));

            remove_file(file_path).expect("Remove file has failed!");
            remove_dir(folder_path).expect("Remove dir has failed!");
        }

        #[test]
        fn test_get_db_sst_names() {
            let db_name: String = "sstNameTestDB".to_string();
            let folder_path_string: String = format!("./{}/", db_name);
            let folder_path: &str = folder_path_string.as_str();

            let mut expected: Vec<String> = vec![];

            create_dir_all(folder_path).expect("Create dir all has failed!");
            for i in 0..10 {
                let file_name: String = format!("output_{}.bin", i);
                File::create(format!("{}{}", folder_path, file_name)).expect("File create failed!");
                expected.insert(0, format!("{}{}", folder_path, file_name));
            }

            let names: Vec<String> = get_sst_names(&db_name);
            assert_eq!(names, expected);

            for i in 0..10 {
                remove_file(format!("{}output_{}.bin", folder_path, i))
                    .expect("Remove file has failed!");
            }
            remove_dir(folder_path).expect("Remove dir has failed!");
        }

        #[test]
        fn test_get_from_kv_arr_binary_search() {
            let mut kv_vec: Vec<(i64, i64)> = Vec::new();
            for i in 0..100 {
                kv_vec.push((i, i * 2));
            }

            for key in 0..100 {
                assert_eq!(Some(key * 2), binary_search_array(&kv_vec, key));
            }
            assert_eq!(None, binary_search_array(&kv_vec, 100));
        }

        #[test]
        fn test_get_from_sst_binary_search() {
            let folder_path: &str = "./getBinarySearchTestDB1/";
            let file_path_string: String = format!("{}output_1.bin", folder_path);
            let file_path: &str = file_path_string.as_str();

            create_dir_all(folder_path).expect("Create dir all has failed!");

            let mut kv_vec: Vec<(i64, i64)> = Vec::new();
            for i in 0..((PAGE_SIZE / 16) * 5) as i64 {
                kv_vec.push((i, i * 2));
            }
            serialize_kv_to_file(file_path, &kv_vec);

            let file_size: usize =
                (metadata(file_path).expect("Metadata call failed!").len() as usize) / PAGE_SIZE;

            for key in 0..((PAGE_SIZE / 16) * 5) as i64 {
                assert_eq!(Some(key * 2), binary_search_file(file_path, file_size, key));
            }

            assert_eq!(
                None,
                binary_search_file(file_path, file_size, ((PAGE_SIZE / 16) * 5) as i64)
            );

            remove_file(file_path).expect("Remove file has failed!");
            remove_dir(folder_path).expect("Remove dir has failed!");
        }

        #[test]
        fn test_get_from_ssts_binary_search() {
            let db_name: String = "getBinarySearchTestDB2".to_string();
            let db_path: String = format!("./{}/", &db_name);

            let pages: i64 = ((PAGE_SIZE / 16) * 5) as i64;

            create_dir_all(&db_path).expect("Create dir all has failed!");

            for i in 0..5 {
                let file_path: String = format!("{}output_{}.bin", &db_path, i);
                let mut kv_vec: Vec<(i64, i64)> = Vec::new();
                for j in i * pages..(i + 1) * pages {
                    kv_vec.push((j, j * 2));
                }
                if i < 4 {
                    let mut key: i64 = ((i + 1) * pages) + 10;
                    kv_vec.push((key, key * 3));
                    key += 100;
                    kv_vec.push((key, key * 3));
                }
                serialize_kv_to_file(&file_path, &kv_vec);
            }

            for i in 0..5 {
                for j in i * pages..(i + 1) * pages {
                    assert_eq!(Some(j * 2), get_value_ssts(&db_name, j));
                }
            }

            assert_eq!(
                None,
                get_value_ssts(&db_name, (((PAGE_SIZE / 16) * 5) * 5) as i64)
            );

            for i in 0..5 {
                remove_file(format!("{}output_{}.bin", &db_path, i))
                    .expect("Remove file has failed!");
            }
            remove_dir(db_path).expect("Remove dir has failed!");
        }

        #[test]
        fn test_scan_get_start_index_from_kv_arr_binary_search() {
            let mut kv_vec: Vec<(i64, i64)> = Vec::new();
            for i in (6..300).step_by(3) {
                kv_vec.push((i, i));
            }

            for key in 6..298 {
                assert_eq!(
                    Some((3 + (key - 6) - 1) / 3),
                    binary_search_array_start_index(&kv_vec, key as i64)
                );
            }

            assert_eq!(None, binary_search_array_start_index(&kv_vec, 300));
        }

        #[test]
        fn test_scan_get_start_idx_from_sst_binary_search() {
            let folder_path: String = "./scanBinarySearchTestDB1/".to_string();
            let file_path: String = format!("{}output_1.bin", &folder_path);

            create_dir_all(&folder_path).expect("Create dir all has failed!");

            let mut kv_vec: Vec<(i64, i64)> = Vec::new();
            for i in 10..(((PAGE_SIZE / 16) * 5) + 10) as i64 {
                kv_vec.push((i, i));
            }
            serialize_kv_to_file(&file_path, &kv_vec);

            let total_pages: usize =
                (metadata(&file_path).expect("Metadata call failed!").len() as usize) / PAGE_SIZE;

            assert_eq!(5, total_pages);

            let mut start: i64 = 20;
            let mut end: i64 = 40;
            assert_eq!(
                (Some(0), Some(10)),
                binary_search_sst_start_index(&file_path, &total_pages, start, end)
            );

            start = 20 + ((PAGE_SIZE / 16) * 2) as i64;
            end = 40 + ((PAGE_SIZE / 16) * 4) as i64;
            assert_eq!(
                (Some(2), Some(10)),
                binary_search_sst_start_index(&file_path, &total_pages, start, end)
            );

            start = 2;
            end = 40;
            assert_eq!(
                (Some(0), Some(0)),
                binary_search_sst_start_index(&file_path, &total_pages, start, end)
            );

            start = 1;
            end = 5;
            assert_eq!(
                (None, None),
                binary_search_sst_start_index(&file_path, &total_pages, start, end)
            );

            remove_file(file_path).expect("Remove file has failed!");
            remove_dir(folder_path).expect("Remove dir has failed!");
        }

        #[test]
        fn test_scan_from_sst_binary_search() {
            let folder_path: String = format!("./scanBinarySearchTestDB2/");
            let file_path: String = format!("{}output_1.bin", &folder_path);

            create_dir_all(&folder_path).expect("Create dir all has failed!");

            let start1: i64 = (((PAGE_SIZE / 16) * 2) + 10) as i64;
            let end1: i64 = (((PAGE_SIZE / 16) * 4) + 20) as i64;
            let start2: i64 = (((PAGE_SIZE / 16) * 4) + 10) as i64;
            let end2: i64 = (((PAGE_SIZE / 16) * 7) + 20) as i64;
            let start3: i64 = ((PAGE_SIZE / 16) * 2) as i64;
            let end3: i64 = (((PAGE_SIZE / 16) * 3) - 1) as i64;

            let mut kv_vec: Vec<(i64, i64)> = Vec::new();
            let mut kv_expected1: HashMap<i64, i64> = HashMap::new();
            let mut kv_expected2: HashMap<i64, i64> = HashMap::new();
            for i in 0..((PAGE_SIZE / 16) * 2) as i64 {
                if i % 3 == 0 {
                    kv_vec.push((i, i * 2));
                }
                kv_vec.push((i, i * 2));
                if start1 <= i && i <= end1 {
                    kv_expected1.insert(i, i * 2);
                }
                if start2 <= i && i <= end2 {
                    kv_expected2.insert(i, i * 2);
                }
            }
            for i in ((PAGE_SIZE / 16) * 3) as i64..((PAGE_SIZE / 16) * 6) as i64 {
                if i % 3 == 0 {
                    kv_vec.push((i, i * 2));
                }
                kv_vec.push((i, i * 2));
                if start1 <= i && i <= end1 {
                    kv_expected1.insert(i, i * 2);
                }
                if start2 <= i && i <= end2 {
                    kv_expected2.insert(i, i * 2);
                }
            }
            serialize_kv_to_file(&file_path, &kv_vec);

            let total_pages: usize =
                (metadata(&file_path).expect("Metadata call failed!").len() as usize) / PAGE_SIZE;

            let mut kv_ret1: HashMap<i64, i64> = HashMap::new();
            if let (Some(page_idx), Some(arr_idx)) =
                binary_search_sst_start_index(&file_path, &total_pages, start1, end1)
            {
                scan_file(
                    &file_path,
                    total_pages,
                    page_idx,
                    arr_idx,
                    end1,
                    &mut kv_ret1,
                );
            } else {
                assert!(false, "Not supposed to get here!");
            }

            assert_eq!(
                kv_expected1.len(),
                kv_ret1.len(),
                "Expected: {}. Got: {}.",
                kv_expected1.len(),
                kv_ret1.len()
            );
            for (key, value) in kv_expected1 {
                if let Some(val) = kv_ret1.get(&key) {
                    if *val != value {
                        assert!(false);
                    }
                } else {
                    assert!(false);
                }
            }

            let mut kv_ret2: HashMap<i64, i64> = HashMap::new();
            if let (Some(page_idx), Some(arr_idx)) =
                binary_search_sst_start_index(&file_path, &total_pages, start2, end2)
            {
                scan_file(
                    &file_path,
                    total_pages,
                    page_idx,
                    arr_idx,
                    end2,
                    &mut kv_ret2,
                );
            } else {
                assert!(false, "Not supposed to get here!");
            }

            assert_eq!(
                kv_expected2.len(),
                kv_ret2.len(),
                "Expected: {}. Got: {}.",
                kv_expected2.len(),
                kv_ret2.len()
            );
            for (key, value) in kv_expected2 {
                if let Some(val) = kv_ret2.get(&key) {
                    if *val != value {
                        assert!(false);
                    }
                } else {
                    assert!(false);
                }
            }

            let mut kv_ret3: HashMap<i64, i64> = HashMap::new();
            if let (Some(page_idx), Some(arr_idx)) =
                binary_search_sst_start_index(&file_path, &total_pages, start3, end3)
            {
                print!("{}, {}", page_idx, arr_idx);
                scan_file(
                    &file_path,
                    total_pages,
                    page_idx,
                    arr_idx,
                    end3,
                    &mut kv_ret3,
                );
                assert!(kv_ret3.is_empty());
            }

            remove_file(&file_path).expect("Remove file has failed!");
            remove_dir(&folder_path).expect("Remove dir has failed!");
        }

        #[test]
        fn test_scan_from_ssts_binary_search() {
            let db_name: String = "scanBinarySearchTestDB3".to_string();
            let db_path: String = format!("./{}/", db_name);

            let pages: i64 = ((PAGE_SIZE / 16) * 5) as i64;

            create_dir_all(&db_path).expect("Create dir all has failed!");

            let start1: i64 = (((PAGE_SIZE / 16) * 2) + 10) as i64;
            let end1: i64 = (((PAGE_SIZE / 16) * 20) + 20) as i64;
            let start2: i64 = 0;
            let end2: i64 = (pages * 5) - 1;
            let start3: i64 = -10;
            let end3: i64 = -1;

            let mut kv_expected1: HashMap<i64, i64> = HashMap::new();
            let mut kv_expected2: HashMap<i64, i64> = HashMap::new();
            for i in 0..5 {
                let file_path: String = format!("{}output_{}.bin", &db_path, i);
                let mut kv_vec: Vec<(i64, i64)> = Vec::new();
                for j in i * pages..(i + 1) * pages {
                    kv_vec.push((j, j * 2));
                    if start1 <= j && j <= end1 {
                        kv_expected1.insert(j, j * 2);
                    }
                    kv_expected2.insert(j, j * 2);
                }
                if i < 4 {
                    let mut key: i64 = ((i + 1) * pages) + 10;
                    kv_vec.push((key, key * 3));
                    key += 100;
                    kv_vec.push((key, key * 3));
                }
                serialize_kv_to_file(&file_path, &kv_vec);
            }

            let mut kv_ret1: HashMap<i64, i64> = HashMap::new();
            scan_ssts(&db_name, start1, end1, &mut kv_ret1);
            assert_eq!(
                kv_expected1.len(),
                kv_ret1.len(),
                "Expected: {}. Got: {}.",
                kv_expected1.len(),
                kv_ret1.len()
            );
            for (key, value) in kv_expected1 {
                if let Some(val) = kv_ret1.get(&key) {
                    if *val != value {
                        assert!(false);
                    }
                } else {
                    assert!(false);
                }
            }

            let mut kv_ret2: HashMap<i64, i64> = HashMap::new();
            scan_ssts(&db_name, start2, end2, &mut kv_ret2);
            assert_eq!(
                kv_expected2.len(),
                kv_ret2.len(),
                "Expected: {}. Got: {}.",
                kv_expected2.len(),
                kv_ret2.len()
            );
            for (key, value) in kv_expected2 {
                if let Some(val) = kv_ret2.get(&key) {
                    if *val != value {
                        assert!(false);
                    }
                } else {
                    assert!(false);
                }
            }

            let mut kv_ret3: HashMap<i64, i64> = HashMap::new();
            scan_ssts(&db_name, start3, end3, &mut kv_ret3);
            assert!(kv_ret3.is_empty());

            for i in 0..5 {
                remove_file(format!("{}output_{}.bin", &db_path, i))
                    .expect("Remove file has failed!");
            }
            remove_dir(&db_path).expect("Remove dir has failed!");
        }
    }
}
