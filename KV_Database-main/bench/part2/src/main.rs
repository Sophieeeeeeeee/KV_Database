use std::time::SystemTime;

use kv::{Client, KVConfig, StorageType};
use rand::{prelude::ThreadRng, seq::SliceRandom, thread_rng, Rng};

const SAMPLES: u128 = 1024;
const SIZES: u32 = 11;

fn put_bench(db: &mut Client) {
    let mut r: ThreadRng = thread_rng();
    let numbers: Vec<(i64, i64)> = (0..SAMPLES * 256)
        .map(|_| (r.gen::<i64>(), r.gen::<i64>()))
        .collect();
    let start: SystemTime = SystemTime::now();
    for (a, b) in numbers {
        db.put(a, b);
    }
    let finish: u128 = start.elapsed().unwrap().as_nanos();
    println!(
        "{} Random PUTs took {} nanoseconds. Throughput of {} PUTs / second",
        SAMPLES * 256,
        finish,
        SAMPLES * 256 * 1_000_000_000 / finish
    );
}

fn get_bench(db: &mut Client, valid_keys: &Vec<i64>) {
    let mut r: ThreadRng = thread_rng();
    let numbers: Vec<i64> = valid_keys
        .choose_multiple(&mut r, SAMPLES as usize)
        .cloned()
        .collect();
    let start: SystemTime = SystemTime::now();
    for a in numbers {
        db.get(a);
    }
    let finish: u128 = start.elapsed().unwrap().as_nanos();
    println!(
        "{} Random GETs took {} nanoseconds. Throughput of {} GETs / second",
        SAMPLES,
        finish,
        SAMPLES * 1_000_000_000 / finish
    );
}

fn scan_bench(db: &mut Client, valid_keys: &Vec<i64>, range: i64) {
    let half_range: i64 = range / 2;
    let mut r: ThreadRng = thread_rng();
    let numbers: Vec<i64> = valid_keys
        .choose_multiple(&mut r, SAMPLES as usize)
        .cloned()
        .collect();
    let start: SystemTime = SystemTime::now();
    for a in numbers {
        db.scan(a - half_range, a + half_range);
    }
    let finish: u128 = start.elapsed().unwrap().as_nanos();
    println!(
        "{} Random SCANs of range {} took {} nanoseconds. Throughput of {} SCANs / second",
        SAMPLES,
        range,
        finish,
        SAMPLES * 1_000_000_000 / finish
    );
}

fn insert_data(db: &mut Client, mb: usize) -> Vec<i64> {
    let mut r: ThreadRng = rand::thread_rng();
    let mut ret: Vec<i64> = Vec::with_capacity(mb * 256 * 256);
    for _ in 0..mb * 256 * 256 {
        let (key, value) = (r.gen::<i64>(), r.gen::<i64>());
        ret.push(key);
        db.put(key, value);
    }
    ret
}

fn main() {
    for i in 0..SIZES {
        let mut db: Client = Client::open(
            format!("part_2_bench_{}", i).to_string(),
            KVConfig::default()
                .memtable_size(256 * 256)
                .storage_type(StorageType::BTree)
                .cleanup(true),
        );
        let valid_keys: Vec<i64> = insert_data(&mut db, 2_usize.pow(i));
        println!("DB Size of {}MB:", 2_i64.pow(i));
        get_bench(&mut db, &valid_keys);
        scan_bench(&mut db, &valid_keys, 100);
        put_bench(&mut db);
    }
}
