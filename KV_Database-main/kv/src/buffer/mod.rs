mod lru;

use crate::buffer::lru::{LRUMain, LRUNode};
use crate::serde::deserialize_page;
use std::{
    cell::{Ref, RefCell, RefMut},
    rc::{Rc, Weak},
};
use twox_hash::xxh3::hash64;

/// Struct to represent the key of a page in the buffer.
pub struct BufferKey {
    /// The name of the SST it belongs to.
    sst_name: String,
    /// The offset in bytes of where it is in the SST.
    page_offset: usize,
}

/// Struct to represent a node in the BufferPool's `buffer`.
pub struct BufferNode {
    /// The key of that node's page.
    key: BufferKey,
    /// The content of the page.
    page: Vec<(i64, i64)>,
    /// A reference to the `LRUNode` that represents this `BufferNode`.
    lru_node: Weak<RefCell<LRUNode>>,
    /// A reference to the next `BufferNode` in the potential chain given collision.
    next: Option<Rc<RefCell<BufferNode>>>,
    /// A reference to the previous `BufferNode` in the potential chain given collision.
    prev: Option<Rc<RefCell<BufferNode>>>,
}

/// Struct to represent the buffer pool for the `Client` structure.
pub struct BufferPool {
    /// The max allowed size of the `buffer`.
    size: usize,
    /// The current size of the `buffer`.
    curr_size: usize,
    /// The buffer's hash representation.
    buffer: Vec<Option<Rc<RefCell<BufferNode>>>>,
    /// The buffer's LRU representation (used to know order of eviction).
    lru: LRUMain,
}

/// Helper function to hash a `BufferKey` into a usize to know where it belongs in the `BufferPool`'s `buffer`.
/// # Arguments
/// * `key` - The `BufferKey` to hash.
/// * `arr_size` - The max size of the buffer hash array to not overflow.
fn custom_hash(key: &BufferKey, arr_size: usize) -> usize {
    let combined: String = format!("{} {}", key.sst_name, key.page_offset);
    let data: &[u8] = combined.as_bytes();
    let hashed: u64 = hash64(data);
    (hashed % (arr_size as u64)) as usize
}

/// Helper to search down the potential chain in the `BufferPool`'s `buffer`. Returns a reference to the `BufferNode`
/// if found.
/// # Arguments
/// * `key` - `BufferKey` to find the corresponding node of.
/// * `curr_node` - A pointer to the current node in the recursive search call.
fn search_buffer_chain(
    key: &BufferKey,
    curr_node: &Option<Rc<RefCell<BufferNode>>>,
) -> Option<Rc<RefCell<BufferNode>>> {
    match curr_node {
        Some(node) => {
            let borrowed: Ref<'_, BufferNode> = node.borrow();
            if borrowed.key == *key {
                return Some(node.clone());
            }
            search_buffer_chain(key, &borrowed.next)
        }
        None => None,
    }
}

// Implementation of `BufferKey`.
impl BufferKey {
    /// Creating a new `BufferKey` given the `sst_name` and `page_offset`.
    /// # Arguments
    /// * `sst_name` - The name of the SST the page to represent belongs in.
    /// * `page_offset` - The offset to get to the page once in the SST.
    pub fn new(sst_name: String, page_offset: usize) -> Self {
        BufferKey {
            sst_name,
            page_offset,
        }
    }
}

// Special implementation of `BufferKey`. To check equality.
impl PartialEq for BufferKey {
    /// Check content equality between two keys. Returns `true` if they match. `false` otherwise.
    /// # Arguments
    /// * `self` - One key.
    /// * `other` - The other key.
    fn eq(&self, other: &Self) -> bool {
        self.sst_name == other.sst_name && self.page_offset == other.page_offset
    }
}

// Implementation of `BufferNode`.
impl BufferNode {
    /// Creating a new `BufferNode` given the `key`, `page` content, and `lru_node`.
    /// # Arguments
    /// * `key` - The `BufferKey` to represent the node.
    /// * `page` - The contents of the page that the node represents.
    /// * `lru_node` - A ref to the LRU node that matches this new node.
    pub fn new(key: BufferKey, page: Vec<(i64, i64)>, lru_node: Rc<RefCell<LRUNode>>) -> Self {
        BufferNode {
            key,
            page,
            lru_node: Rc::downgrade(&lru_node),
            next: None,
            prev: None,
        }
    }

    /// Returns a copy of the page's data.
    fn get_page_data(&self) -> Vec<(i64, i64)> {
        self.page.clone()
    }
}

// Implementation of `BufferPool`.
impl BufferPool {
    /// Creating a new `BufferPool` given a `buffer_size`. Initialize the buffer to None, current size to zero, and make
    /// a new `LRUMain` object.
    /// # Arguments
    /// * `buffer_size` - The size of the buffer to initialize.
    pub fn new(buffer_size: usize) -> Self {
        let mut buf: Vec<Option<Rc<RefCell<BufferNode>>>> = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buf.push(None);
        }
        BufferPool {
            size: buffer_size,
            curr_size: 0,
            buffer: buf,
            lru: LRUMain::new(),
        }
    }

    /// The primary function for outside functions that use a buffer to call. It will check the buffer for the requested data
    /// and if it is not found it will get it from storage and add it to itself before returning the data.
    /// # Arguments
    /// * `self` - The buffer object.
    /// * `sst_name` - The name of the SST the requested page belongs to.
    /// * `page_offset` - The offset to find the requested page in the SST.
    pub fn find_page(&mut self, sst_name: &str, page_offset: usize) -> Vec<(i64, i64)> {
        let key: BufferKey = BufferKey::new(sst_name.to_string(), page_offset);

        if let Some(page) = self.find_buffer_page(&key) {
            return page;
        }

        let page: Vec<(i64, i64)> = deserialize_page(&key.sst_name, key.page_offset);
        self.insert(key, page.clone());

        page
    }

    /// The helper function called by `find_page` to call the search through the buffer before going to storage
    /// and call the LRU update function if page was found.
    /// # Arguments
    /// * `self` - A mutable buffer ref to be able to update the LRU queue when a page is found.
    /// * `key` - The `BufferKey` to use in the search.
    fn find_buffer_page(&mut self, key: &BufferKey) -> Option<Vec<(i64, i64)>> {
        if let Some(good_node) = self.search_buffer(key) {
            let page: Vec<(i64, i64)>;
            {
                let good_node_ref: Ref<'_, BufferNode> = good_node.borrow();
                page = good_node_ref.get_page_data();

                self.lru
                    .update_lru_position(good_node_ref.lru_node.upgrade().unwrap());
            }

            drop(good_node);
            return Some(page);
        }
        None
    }

    /// The helper function called by `find_page` to insert the new page into the buffer when it was requested and
    /// not already buffered.
    /// # Arguments
    /// * `self` - A mutable buffer ref to be able to update the buffer with the new nodes (`LRUNode` and `BufferNode`).
    /// * `key` - The `BufferKey` to use in the insert for hashing.
    /// * `page` - The content of the new page to add to buffer.
    fn insert(&mut self, key: BufferKey, page: Vec<(i64, i64)>) {
        if self.curr_size == self.size && !self.run_eviction() {
            panic!("Eviction failed when attempting overflow insert!");
        }

        let hash: usize = custom_hash(&key, self.size);

        let lru_node: Rc<RefCell<LRUNode>> = Rc::new(RefCell::new(LRUNode::new(Weak::new())));
        let new_node: Rc<RefCell<BufferNode>> =
            Rc::new(RefCell::new(BufferNode::new(key, page, lru_node.clone())));
        lru_node.borrow_mut().set_data(Rc::downgrade(&new_node));
        self.lru.add_node(lru_node);

        match self.buffer[hash].take() {
            Some(old_root) => {
                {
                    let mut old_root_ref: RefMut<'_, BufferNode> = old_root.borrow_mut();
                    old_root_ref.prev = Some(new_node.clone());
                }
                {
                    let mut new_node_ref: RefMut<'_, BufferNode> = new_node.borrow_mut();
                    new_node_ref.next = Some(old_root);
                }
                self.buffer[hash] = Some(new_node);
            }
            None => self.buffer[hash] = Some(new_node),
        };

        self.curr_size += 1;
    }

    /// The helper function called by `find_buffer_page` to do the searching for the page in the buffer.
    /// # Arguments
    /// * `self` - A ref to the `BufferPool` object.
    /// * `key` - The `BufferKey` to find.
    fn search_buffer(&self, key: &BufferKey) -> Option<Rc<RefCell<BufferNode>>> {
        if self.size == 0 {
            return None;
        }

        let hash: usize = custom_hash(key, self.size);

        match &self.buffer[hash] {
            Some(node) => {
                let borrowed: Ref<'_, BufferNode> = node.borrow();
                if borrowed.key == *key {
                    return Some(node.clone());
                }
                search_buffer_chain(key, &borrowed.next)
            }
            None => None,
        }
    }

    /// The helper function called by `insert` to evict a page when the buffer has reached max size and still needs to insert
    /// a new entry. Returns `true` when it successfully evicted an entry. `false` otherwise.
    /// # Arguments
    /// * `self` - A mutable ref to the `BufferPool` object for manipulation of the `buffer` and `lru`.
    fn run_eviction(&mut self) -> bool {
        match self.lru.next_to_evict() {
            Some(evict_node) => {
                {
                    let mut evict_node_ref: RefMut<'_, BufferNode> = evict_node.borrow_mut();
                    assert!(evict_node_ref.lru_node.upgrade().is_none());

                    let prev_wrapped: Option<Rc<RefCell<BufferNode>>> = evict_node_ref.prev.take();
                    let next_wrapped: Option<Rc<RefCell<BufferNode>>> = evict_node_ref.next.take();

                    if let Some(prev) = &prev_wrapped {
                        let mut prev_ref: RefMut<'_, BufferNode> = prev.borrow_mut();
                        prev_ref.next = next_wrapped.clone();
                    } else {
                        let hash: usize = custom_hash(&evict_node_ref.key, self.size);
                        self.buffer[hash] = next_wrapped.clone();
                    }
                    if let Some(next) = &next_wrapped {
                        let mut next_ref: RefMut<'_, BufferNode> = next.borrow_mut();
                        next_ref.prev = prev_wrapped.clone();
                    }

                    drop(prev_wrapped);
                    drop(next_wrapped);
                }

                drop(evict_node);
                self.curr_size -= 1;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    mod buffer {
        use std::{
            cell::{Ref, RefCell},
            collections::VecDeque,
            rc::Rc,
        };

        use crate::buffer::{BufferKey, BufferNode, BufferPool};

        #[test]
        fn test_buffer_inserts_simple() {
            let buf_size = 5;
            let num_inserts = 5;
            let mut expected: Vec<Vec<(i64, i64)>> = Vec::new();
            let mut buffer: BufferPool = BufferPool::new(buf_size);

            for i in 1..=num_inserts as usize {
                let mut page: Vec<(i64, i64)> = Vec::new();
                for _ in 0..=10 as usize {
                    let num1 = i;
                    let num2 = i;
                    page.push((num1 as i64, num2 as i64));
                }
                expected.push(page.clone());
                buffer.insert(BufferKey::new(format!("sst{}", i), i * 2), page);
            }

            assert_eq!(buffer.curr_size, 5);

            for i in 0..=(buf_size - 1) as usize {
                match buffer.buffer[i].take() {
                    Some(node) => {
                        let borrowed: Ref<'_, BufferNode> = node.borrow();
                        let page: Vec<(i64, i64)> = borrowed.get_page_data();
                        if let Some(idx) = expected.iter().position(|x| *x == page) {
                            expected.remove(idx);
                        } else {
                            assert!(false);
                        }
                        let mut curr_node: Option<Rc<RefCell<BufferNode>>> =
                            borrowed.next.to_owned();
                        while let Some(node) = curr_node {
                            let unwrapped_node: Ref<'_, BufferNode> = node.borrow();
                            let page: Vec<(i64, i64)> = unwrapped_node.get_page_data();
                            if let Some(idx) = expected.iter().position(|x| *x == page) {
                                expected.remove(idx);
                            } else {
                                assert!(false);
                            }

                            curr_node = unwrapped_node.next.to_owned();
                        }
                    }
                    _ => {}
                }
            }
            assert!(expected.is_empty());
        }

        #[test]
        fn test_buffer_inserts_overflow() {
            let buf_size = 5;
            let num_inserts = 7;
            let mut expected: Vec<Vec<(i64, i64)>> = Vec::new();
            let mut overflow: Vec<Vec<(i64, i64)>> = Vec::new();
            let mut buffer: BufferPool = BufferPool::new(buf_size);

            for i in 1..=num_inserts {
                let mut page: Vec<(i64, i64)> = Vec::new();
                for _ in 0..=10 as usize {
                    let num1 = i;
                    let num2 = i;
                    page.push((num1 as i64, num2 as i64));
                }
                if i <= num_inserts - buf_size {
                    overflow.push(page.clone());
                }
                expected.push(page.clone());
                buffer.insert(BufferKey::new(format!("sst{}", i), i * 2), page);
            }

            assert_eq!(buffer.curr_size, buf_size);

            for i in 0..=(buf_size - 1) as usize {
                match buffer.buffer[i].take() {
                    Some(node) => {
                        let borrowed: Ref<'_, BufferNode> = node.borrow();
                        let page: Vec<(i64, i64)> = borrowed.get_page_data();
                        if let Some(idx) = expected.iter().position(|x| *x == page) {
                            expected.remove(idx);
                        } else {
                            assert!(false);
                        }
                        let mut curr_node: Option<Rc<RefCell<BufferNode>>> =
                            borrowed.next.to_owned();
                        while let Some(node) = curr_node {
                            let unwrapped_node: Ref<'_, BufferNode> = node.borrow();
                            let page: Vec<(i64, i64)> = unwrapped_node.get_page_data();
                            if let Some(idx) = expected.iter().position(|x| *x == page) {
                                expected.remove(idx);
                            } else {
                                assert!(false);
                            }

                            curr_node = unwrapped_node.next.to_owned();
                        }
                    }
                    _ => {}
                }
            }

            assert_eq!(expected.len(), (num_inserts - buf_size) as usize);

            for _ in 1..=num_inserts - buf_size {
                assert_eq!(expected.pop(), overflow.pop());
            }
        }

        #[test]
        fn test_buffer_insert_search_simple() {
            let buf_size = 5;
            let num_inserts = 5;
            let mut keys: Vec<BufferKey> = Vec::new();
            let mut expected: Vec<Vec<(i64, i64)>> = Vec::new();
            let mut buffer: BufferPool = BufferPool::new(buf_size);

            for i in 1..=num_inserts as usize {
                let mut page: Vec<(i64, i64)> = Vec::new();
                for _ in 0..=10 as usize {
                    let num1 = i;
                    let num2 = i;
                    page.push((num1 as i64, num2 as i64));
                }
                keys.push(BufferKey::new(format!("sst{}", i), i * 2));
                expected.push(page.clone());
                buffer.insert(BufferKey::new(format!("sst{}", i), i * 2), page);
            }

            assert_eq!(buffer.curr_size, buf_size);

            for i in 0..=(num_inserts - 1) {
                let key: &BufferKey = &keys[i];
                if let Some(page) = buffer.find_buffer_page(key) {
                    assert_eq!(page, expected[i]);
                } else {
                    assert!(false);
                }
            }

            assert_eq!(
                None,
                buffer.find_buffer_page(&BufferKey::new("Wrong_Name".to_string(), 360))
            );
        }

        #[test]
        fn test_buffer_insert_search_overflow() {
            let buf_size = 5;
            let num_inserts = 7;
            let mut keys: Vec<BufferKey> = Vec::new();
            let mut expected: Vec<Vec<(i64, i64)>> = Vec::new();
            let mut buffer: BufferPool = BufferPool::new(buf_size);

            for i in 1..=num_inserts as usize {
                let mut page: Vec<(i64, i64)> = Vec::new();
                for _ in 0..=10 as usize {
                    let num1 = i;
                    let num2 = i;
                    page.push((num1 as i64, num2 as i64));
                }
                keys.push(BufferKey::new(format!("sst{}", i), i * 2));
                if i > num_inserts - buf_size {
                    expected.push(page.clone());
                }
                buffer.insert(BufferKey::new(format!("sst{}", i), i * 2), page);
            }

            assert_eq!(buffer.curr_size, buf_size);

            for i in 0..=num_inserts - 1 {
                let key: &BufferKey = &keys[i as usize];
                let ret = buffer.find_buffer_page(key);
                if i > num_inserts - buf_size - 1 {
                    assert!(ret.is_some());
                    assert_eq!(
                        ret.unwrap(),
                        expected[(i - (num_inserts - buf_size)) as usize]
                    );
                } else {
                    assert_eq!(ret, None);
                }
            }
        }

        #[test]
        fn test_buffer_search_change() {
            let buf_size = 5;
            let num_inserts = 7;
            let num_search = buf_size / 2;
            let mut keys: Vec<BufferKey> = Vec::new();
            let mut keys_expected: VecDeque<BufferKey> = VecDeque::new();
            let mut pages: Vec<Vec<(i64, i64)>> = Vec::new();
            let mut buffer: BufferPool = BufferPool::new(buf_size);

            for i in 1..=buf_size as usize {
                let mut page: Vec<(i64, i64)> = Vec::new();
                for _ in 0..=10 as usize {
                    let num1 = i;
                    let num2 = i;
                    page.push((num1 as i64, num2 as i64));
                }
                keys.push(BufferKey::new(format!("sst{}", i), i * 2));
                keys_expected.push_back(BufferKey::new(format!("sst{}", i), i * 2));
                pages.push(page.clone());
                buffer.insert(BufferKey::new(format!("sst{}", i), i * 2), page);
            }

            assert_eq!(buffer.curr_size, buf_size);

            for i in 0..=(num_search - 1) as usize {
                let key: &BufferKey = &keys[i];
                let ret: Option<Vec<(i64, i64)>> = buffer.find_buffer_page(key);
                assert!(ret.is_some());
                assert_eq!(ret.unwrap(), pages[i]);
                let x: Option<BufferKey> = keys_expected.pop_front();
                keys_expected.push_back(x.unwrap())
            }

            assert_eq!(buffer.curr_size, buf_size);

            for i in buf_size + 1..=num_inserts as usize {
                let mut page: Vec<(i64, i64)> = Vec::new();
                for _ in 0..=10 as usize {
                    let num1 = i;
                    let num2 = i;
                    page.push((num1 as i64, num2 as i64));
                }
                keys.push(BufferKey::new(format!("sst{}", i), i * 2));
                pages.push(page.clone());
                keys_expected.push_back(BufferKey::new(format!("sst{}", i), i * 2));
                keys_expected.pop_front();
                buffer.insert(BufferKey::new(format!("sst{}", i), i * 2), page);
            }

            assert_eq!(buffer.curr_size, buf_size);
            assert_eq!(keys_expected.len(), buf_size as usize);

            for i in 0..=(num_inserts - 1) as usize {
                let key: &BufferKey = &keys[i];
                let ret: Option<Vec<(i64, i64)>> = buffer.find_buffer_page(key);
                if keys_expected.contains(key) {
                    assert!(ret.is_some());
                    assert_eq!(ret.clone().unwrap(), pages[i]);
                } else {
                    assert_eq!(ret, None);
                }
            }
        }
    }
}
