use kv;

#[test]
fn lsm_get() {
    let mut db = kv::Client::open(
        "lsm_test".to_string(),
        kv::KVConfig::default()
            .memtable_size(256 * 256)
            .storage_type(kv::StorageType::LSMTree)
            .cleanup(true),
    );

    let c = 1_000_000;
    for i in 0..c {
        db.put(i, i + 5);
    }

    for i in 0..c {
        assert_eq!(db.get(i).unwrap(), i + 5);
    }
}

#[test]
fn lsm_scan() {
    let mut db = kv::Client::open(
        "lsm_scan".to_string(),
        kv::KVConfig::default()
            .memtable_size(256 * 256)
            .storage_type(kv::StorageType::LSMTree)
            .cleanup(true),
    );

    let c = 1_000_000;
    for i in 0..c {
        db.put(i, i + 5);
    }

    let a = db.scan(0, 30);

    assert!(a.len() == 31);
    for pair in a {
        assert!(pair.0 + 5 == pair.1)
    }
}
