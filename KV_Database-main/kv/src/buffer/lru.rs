use crate::buffer::BufferNode;
use std::{
    cell::{Ref, RefCell, RefMut},
    rc::{Rc, Weak},
};

/// Struct to represent a node in `LRUMain` (`lru` in `BufferPool`).
pub struct LRUNode {
    /// A ref to the `BufferNode` that contains the data of the page.
    data: Weak<RefCell<BufferNode>>,
    /// A ref to the next `LRUNode` in the chain.
    next: Option<Rc<RefCell<LRUNode>>>,
    /// A ref to the previous `LRUNode` in the chain.
    prev: Option<Rc<RefCell<LRUNode>>>,
}

/// Struct to represent the main body of the LRU (`lru` in `BufferPool`).
pub struct LRUMain {
    /// A ref to the front of the LRU. Next to evict.
    front_q: Option<Rc<RefCell<LRUNode>>>,
    /// A ref to the back of the LRU. Where to add new.
    back_q: Option<Rc<RefCell<LRUNode>>>,
}

/// Helper function to check if two refs point to the same node. Returns `true` if they match. `false` otherwise.
/// # Arguments
/// * `node1` - A ref to a node.
/// * `node2` - A ref to the other node.
fn ref_eq<T>(node1: Option<Rc<RefCell<T>>>, node2: Option<Rc<RefCell<T>>>) -> bool {
    if node1.is_none() || node2.is_none() {
        return false;
    }
    Rc::ptr_eq(&node1.unwrap(), &node2.unwrap())
}

// Implementation of `LRUNode`.
impl LRUNode {
    /// Creating a new `LRUNode` given a ref to the corresponding `data`.
    /// # Arguments
    /// * `data` - The ref to the corresponding `BufferNode`.
    pub fn new(data: Weak<RefCell<BufferNode>>) -> Self {
        LRUNode {
            data,
            next: None,
            prev: None,
        }
    }

    /// Function to set the ref to the corresponding `BufferNode`.
    /// # Arguments
    /// * `self` - A mutable ref to the `LRUNode` object to update.
    /// * `data` - A ref to the corresponding `BufferNode`.
    pub fn set_data(&mut self, data: Weak<RefCell<BufferNode>>) {
        self.data = data;
    }
}

// Implementation of `LRUMain`.
impl LRUMain {
    /// Creating a new `LRUMain` that starts empty.
    pub fn new() -> Self {
        LRUMain {
            front_q: None,
            back_q: None,
        }
    }

    /// The helper function called by `find_buffer_page` to do the LRU updating for the page in the buffer.
    /// Should set the accessed page at the back of the LRU.
    /// # Arguments
    /// * `self` - A mutable ref to the `LRUMain` object to update the positioning.
    /// * `node` - A ref to the `LRUNode` which was accessed.
    pub fn update_lru_position(&mut self, node: Rc<RefCell<LRUNode>>) {
        if self.is_empty() {
            panic!("LRU should not be empty at this point!");
        }
        if ref_eq(self.back_q.clone(), Some(node.clone())) {
            return;
        }

        {
            let mut node_ref: RefMut<'_, LRUNode> = node.borrow_mut();
            let prev_wrapped: Option<Rc<RefCell<LRUNode>>> = node_ref.prev.take();
            let next_wrapped: Option<Rc<RefCell<LRUNode>>> = node_ref.next.take();

            if let Some(prev) = &prev_wrapped {
                let mut prev_ref: RefMut<'_, LRUNode> = prev.borrow_mut();
                prev_ref.next = next_wrapped.clone();
            }
            if let Some(next) = &next_wrapped {
                let mut next_ref: RefMut<'_, LRUNode> = next.borrow_mut();
                next_ref.prev = prev_wrapped.clone();
            } else {
                self.front_q = prev_wrapped.clone();
            }

            drop(prev_wrapped);
            drop(next_wrapped);
        }

        self.add_node(node);
    }

    /// The function called to perform inserting a node in the `LRUMain`.
    /// # Arguments
    /// * `self` - A mutable ref to the `LRUMain` object to update it with the new node.
    /// * `node` - A ref to the new `LRUNode` to add.
    pub fn add_node(&mut self, node: Rc<RefCell<LRUNode>>) {
        if self.is_empty() {
            self.front_q = Some(Rc::clone(&node));
            self.back_q = Some(node);
        } else {
            let old_back: Rc<RefCell<LRUNode>> = self.back_q.take().unwrap();
            old_back.borrow_mut().prev = Some(Rc::clone(&node));
            self.back_q = Some(Rc::clone(&node));
            node.borrow_mut().next = Some(old_back);
        }
    }

    /// The function called to start the real `BufferNode` eviction process. It evicts the next `LRUNode` to be
    /// evicted and returns the corresponding `BufferNode` to be evicted.
    /// # Arguments
    /// * `self` - A mutable ref to `LRUMain` to peform eviction.
    pub fn next_to_evict(&mut self) -> Option<Rc<RefCell<BufferNode>>> {
        if self.is_empty() {
            return None;
        }
        let old_front: Rc<RefCell<LRUNode>> = self.front_q.take().unwrap();
        let data: Option<Rc<RefCell<BufferNode>>>;
        {
            let old_front_ref: Ref<'_, LRUNode> = old_front.borrow();
            data = old_front_ref.data.upgrade();
        }
        self.front_q = old_front.borrow_mut().prev.take();
        if self.front_q.is_some() {
            let new_front: &Rc<RefCell<LRUNode>> = self.front_q.as_ref().unwrap();
            let mut new_front_ref: RefMut<'_, LRUNode> = new_front.borrow_mut();
            new_front_ref.next = None;
        } else {
            self.back_q = None;
        }

        drop(old_front);
        data
    }

    /// A helper function that returns if the `LRUMain` structure is empty. Return `true` if empty. `false` otherwise.
    fn is_empty(&self) -> bool {
        if self.front_q.is_none() != self.back_q.is_none() {
            panic!("LRU queue front and back mismatch!");
        }
        self.front_q.is_none() && self.back_q.is_none()
    }
}
