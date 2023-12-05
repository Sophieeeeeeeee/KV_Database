use crate::memtable::node::AVLTreeNode;
use std::collections::HashMap;

/*
    The following functions are helper functions for the main ones further below.
*/

/// Helper function to left rotate the AVL tree at `root`.
/// # Arguments
/// * `root` - The root node where to rotate.
fn left_rotate(mut root: Box<AVLTreeNode>) -> Box<AVLTreeNode> {
    let mut return_node = root.right.take().expect("invalid AVL tree");
    root.right = return_node.left.take();
    root.update_height();
    return_node.left = Some(root);
    return_node.update_height();
    return_node
}

/// Helper function to right rotate the AVL tree at `root`.
/// # Arguments
/// * `root` - The root node where to rotate.
fn right_rotate(mut root: Box<AVLTreeNode>) -> Box<AVLTreeNode> {
    let mut return_node = root.left.take().expect("invalid AVL tree");
    root.left = return_node.right.take();
    root.update_height();
    return_node.right = Some(root);
    return_node.update_height();
    return_node
}

/// Helper function to left-right rotate the AVL tree at `root`.
/// # Arguments
/// * `root` - The root node where to rotate.
fn left_right_rotate(mut root: Box<AVLTreeNode>) -> Box<AVLTreeNode> {
    root.left = Some(left_rotate(root.left.take().expect("invalid AVL tree")));
    right_rotate(root)
}

/// Helper function to right-left rotate the AVL tree at `root`.
/// # Arguments
/// * `root` - The root node where to rotate.
fn right_left_rotate(mut root: Box<AVLTreeNode>) -> Box<AVLTreeNode> {
    root.right = Some(right_rotate(root.right.take().expect("invalid AVL tree")));
    left_rotate(root)
}

/// Helper function to balance out the AVL tree at `root`.
/// # Arguments
/// * `root` - The root node where to balance.
/// * `key` - The newly added key which caused the balancing process.
fn balance_avl_tree(root: Box<AVLTreeNode>, key: i64) -> Box<AVLTreeNode> {
    match root.balance_factor() {
        -1..=1 => root,
        2 => {
            if key < root.left.as_ref().expect("invalid AVL tree").key {
                right_rotate(root)
            } else {
                left_right_rotate(root)
            }
        }
        -2 => {
            if key > root.right.as_ref().expect("invalid AVL tree").key {
                left_rotate(root)
            } else {
                right_left_rotate(root)
            }
        }
        _ => panic!("invalid balance factor"),
    }
}

/// Helper function to insert a `key`, `value` starting at at `root`.
/// # Arguments
/// * `root` - The root node where to start the insert process.
/// * `key` - The newly added key.
/// * `value` - The newly added value.
fn insert_value(root: Option<Box<AVLTreeNode>>, key: i64, value: i64) -> (Box<AVLTreeNode>, bool) {
    match root {
        Some(mut node) => match key.cmp(&node.key) {
            std::cmp::Ordering::Equal => {
                node.value = value;
                (node, false)
            }
            std::cmp::Ordering::Less => {
                let (left_node, new_node) = insert_value(node.left, key, value);
                node.left = Some(left_node);
                node.update_height();
                (balance_avl_tree(node, key), new_node)
            }
            std::cmp::Ordering::Greater => {
                let (right_node, new_node) = insert_value(node.right, key, value);
                node.right = Some(right_node);
                node.update_height();
                (balance_avl_tree(node, key), new_node)
            }
        },
        None => (Box::new(AVLTreeNode::new(key, value)), true),
    }
}

/// Helper function to return a value with corresponding `key` starting at at `root`.
/// # Arguments
/// * `root` - The root node where to start the get process.
/// * `key` - The key who's value we want.
fn get_value(root: &Option<Box<AVLTreeNode>>, key: i64) -> Option<i64> {
    root.as_ref().and_then(|node| match key.cmp(&node.key) {
        std::cmp::Ordering::Less => get_value(&node.left, key),
        std::cmp::Ordering::Greater => get_value(&node.right, key),
        std::cmp::Ordering::Equal => Some(node.value),
    })
}

/// Helper function to return values with corresponding range of keys (`start` to `end` INCLUSIVE) starting at at `root`.
/// # Arguments
/// * `root` - The root node where to start the scan process.
/// * `start` - The begining of the scan range (INCLUSIVE).
/// * `end` - The end of the scan range (INCLUSIVE).
/// * `kv_hash` - The HashMap to store the output so we do not have duplicates.
fn scan_tree(
    root: &Option<Box<AVLTreeNode>>,
    start: i64,
    end: i64,
    kv_hash: &mut HashMap<i64, i64>,
) {
    if let Some(node) = root {
        if start < node.key {
            scan_tree(&node.left, start, end, kv_hash);
        }

        if start <= node.key && node.key <= end {
            kv_hash.insert(node.key, node.value);
        }
        scan_tree(&node.right, start, end, kv_hash);
    }
}

/// Helper function to return all values in the AVL tree starting at `root`.
/// # Arguments
/// * `root` - The root node where to start the scan process.
fn scan_all_tree(root: &Option<Box<AVLTreeNode>>) -> Vec<(i64, i64)> {
    match root {
        None => vec![],
        Some(a) => {
            let mut r = scan_all_tree(&a.left);
            r.push((a.key, a.value));
            r.extend(scan_all_tree(&a.right));
            r
        }
    }
}

/*
    The following functions are the main functions of the `AVLTree` implementation.
*/

/// Struct to represent the `AVLTree`.
pub struct AVLTree {
    /// The main root of the AVL tree.
    root: Option<Box<AVLTreeNode>>,
    /// The current size of the AVL tree.
    size: u32,
}

// Implementation of the `AVLTree`.
impl AVLTree {
    /// Creating a new `AVLTree`, initialized to being empty.
    pub fn new() -> Self {
        AVLTree {
            root: None,
            size: 0,
        }
    }

    /// Primary function to insert a KV pair in the `AVLTree` structure.
    /// # Arguments
    /// * `self` - A mutable ref to the `AVLTree` struct to update it with the new node.
    /// * `key` - The new key to insert.
    /// * `value` - The new value to insert with the key.
    pub fn put(&mut self, key: i64, value: i64) {
        let (new_root, new_node) = insert_value(self.root.take(), key, value);
        self.root = Some(new_root);

        if new_node {
            self.size += 1;
        }
    }

    /// Primary function to get a value from the `AVLTree` structure.
    /// # Arguments
    /// * `self` - A ref to the `AVLTree` struct to get the value.
    /// * `key` - The key to search for.
    pub fn get(&self, key: i64) -> Option<i64> {
        get_value(&self.root, key)
    }

    /// Primary function to scan for keys in the `AVLTree` structure. Stores the values in `kv_hash` to
    /// eliminate duplicates. Scan range from `start` to `end` keys INCLUSIVE.
    /// # Arguments
    /// * `self` - A ref to the `AVLTree` struct to get the values.
    /// * `start` - The begining of the scan range (INCLUSIVE).
    /// * `end` - The end of the scan range (INCLUSIVE).
    /// * `kv_hash` - The HashMap to store the output so we do not have duplicates.
    pub fn scan(&self, start: i64, end: i64, kv_hash: &mut HashMap<i64, i64>) {
        scan_tree(&self.root, start, end, kv_hash);
    }

    /// Primary function to return all values in the `AVLTree` starting at `self.root`.
    /// * `self` - A ref to the `AVLTree` struct to get the values.
    pub fn scan_all(&self) -> Vec<(i64, i64)> {
        scan_all_tree(&self.root)
    }

    /// Helper function to get the current size of the AVL tree.
    /// # Arguments
    /// * `self` - A ref to the `AVLTree` struct to get the current size.
    pub fn size(&self) -> u32 {
        self.size
    }
}

// Special default `AVLTree` implementation.
impl Default for AVLTree {
    /// The default `AVLTree` implementation.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    mod avl_rotations {
        use super::super::{
            left_right_rotate, left_rotate, right_left_rotate, right_rotate, AVLTreeNode,
        };

        #[test]
        fn test_left_rotate() {
            let mut test_node = Box::new(AVLTreeNode::new(1, 1));
            test_node.right = Some(Box::new(AVLTreeNode::new(2, 2)));
            test_node.right.as_mut().unwrap().left = Some(Box::new(AVLTreeNode::new(3, 3)));

            let balanced = left_rotate(test_node);
            assert_eq!(balanced.key, 2);
            assert_eq!(balanced.left.as_ref().map_or(0, |x| x.key), 1);
            assert_eq!(balanced.left.unwrap().right.map_or(0, |x| x.key), 3);
        }

        #[test]
        fn test_right_rotate() {
            let mut test_node = Box::new(AVLTreeNode::new(1, 1));
            test_node.left = Some(Box::new(AVLTreeNode::new(2, 2)));
            test_node.left.as_mut().unwrap().right = Some(Box::new(AVLTreeNode::new(3, 3)));

            let balanced = right_rotate(test_node);
            assert_eq!(balanced.key, 2);
            assert_eq!(balanced.right.as_ref().map_or(0, |x| x.key), 1);
            assert_eq!(balanced.right.unwrap().left.map_or(0, |x| x.key), 3);
        }

        #[test]
        fn test_left_right_rotate() {
            // Create the initial nodes
            let mut test_node = Box::new(AVLTreeNode::new(1, 1));
            let mut left_node = Box::new(AVLTreeNode::new(2, 2));
            let right_node = Box::new(AVLTreeNode::new(3, 3));

            // Build the left subtree
            left_node.left = Some(Box::new(AVLTreeNode::new(4, 4)));
            let mut left_right_node = Box::new(AVLTreeNode::new(5, 5));
            left_right_node.left = Some(Box::new(AVLTreeNode::new(6, 6)));
            left_right_node.right = Some(Box::new(AVLTreeNode::new(7, 7)));
            left_node.right = Some(left_right_node);

            // Assign the subtrees to the main node
            test_node.left = Some(left_node);
            test_node.right = Some(right_node);

            // Perform the left-right rotation
            let balanced = left_right_rotate(test_node);

            // Assertions
            assert_eq!(balanced.key, 5);
            let left_subtree = balanced.left.as_ref().unwrap();
            assert_eq!(left_subtree.key, 2);
            assert_eq!(left_subtree.left.as_ref().unwrap().key, 4);
            assert_eq!(left_subtree.right.as_ref().unwrap().key, 6);

            let right_subtree = balanced.right.as_ref().unwrap();
            assert_eq!(right_subtree.key, 1);
            assert_eq!(right_subtree.left.as_ref().unwrap().key, 7);
            assert_eq!(right_subtree.right.as_ref().unwrap().key, 3);
        }

        #[test]
        fn test_right_left_rotate() {
            // Create the initial nodes
            let mut test_node = Box::new(AVLTreeNode::new(1, 1));
            let mut right_node = Box::new(AVLTreeNode::new(2, 2));
            let left_node = Box::new(AVLTreeNode::new(3, 3));

            // Build the right subtree
            right_node.right = Some(Box::new(AVLTreeNode::new(4, 4)));
            let mut right_left_node = Box::new(AVLTreeNode::new(5, 5));
            right_left_node.right = Some(Box::new(AVLTreeNode::new(6, 6)));
            right_left_node.left = Some(Box::new(AVLTreeNode::new(7, 7)));
            right_node.left = Some(right_left_node);

            // Assign the subtrees to the main node
            test_node.right = Some(right_node);
            test_node.left = Some(left_node);

            // Perform the right-left rotation
            let balanced = right_left_rotate(test_node);

            // Assertions
            assert_eq!(balanced.key, 5);
            let right_subtree = balanced.right.as_ref().unwrap();
            assert_eq!(right_subtree.key, 2);
            assert_eq!(right_subtree.right.as_ref().unwrap().key, 4);
            assert_eq!(right_subtree.left.as_ref().unwrap().key, 6);

            let left_subtree = balanced.left.as_ref().unwrap();
            assert_eq!(left_subtree.key, 1);
            assert_eq!(left_subtree.right.as_ref().unwrap().key, 7);
            assert_eq!(left_subtree.left.as_ref().unwrap().key, 3);
        }
    }

    mod avl_tree {
        use super::super::AVLTree;

        #[test]
        fn test_insert_and_get_value() {
            let mut tree: AVLTree = AVLTree::new();
            tree.put(1, 2);

            let stored_value: i64 = tree.get(1).expect("key should exist");
            assert_eq!(stored_value, 2);
        }

        #[test]
        fn test_get_invalid_key() {
            let mut tree = AVLTree::new();
            tree.put(1, 2);

            let stored_value = tree.get(4);
            assert!(stored_value.is_none());
        }

        #[test]
        fn test_avl_tree_repeated_puts() {
            let mut tree = AVLTree::new();

            for i in 0..=127 {
                tree.put(i, i);
            }

            for i in 0..=127 {
                assert_eq!(tree.get(i).unwrap(), i);
            }
        }

        // #[test]
        // fn test_scan_range_in_tree() {
        //     let mut tree = AVLTree::new();
        //     for i in 0..=127 {
        //         tree.put(i, i);
        //     }
        //     let mut kv_hash: FxHashMap<i64, i64> = FxHashMap::default();
        //     let output_lst = tree.scan(99, 113, &mut kv_hash);
        //     let mut j: i64 = 99;
        //     for tup in kv_hash.iter() {
        //         assert_eq!(*tup, (j, j));
        //         j += 1;
        //     }
        //     assert_eq!(j, 114)
        // }
        //
        // #[test]
        // fn test_scan_range_not_in_tree() {
        //     let mut tree = AVLTree::new();
        //     for i in 99..=127 {
        //         tree.put(i, i);
        //     }
        //     let mut kv_hash: FxHashMap<i64, i64> = FxHashMap::default();
        //     let output_lst = tree.scan(0, 98, &mut kv_hash);
        //     assert_eq!(*output_lst, Vec::<(i64, i64)>::new())
        // }

        #[test]
        fn test_avl_tree_size_none() {
            let tree = AVLTree::new();
            assert_eq!(tree.size(), 0)
        }

        #[test]
        fn test_avl_tree_size_large() {
            let mut tree = AVLTree::new();

            for i in 0..=127 {
                tree.put(i, i);
            }

            assert_eq!(tree.size(), 128)
        }
    }
}
