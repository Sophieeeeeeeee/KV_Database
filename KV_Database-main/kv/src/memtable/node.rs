/// Struct for an `AVLTreeNode`.
pub struct AVLTreeNode {
    /// The key of the node.
    pub key: i64,
    /// The value of the node.
    pub value: i64,
    /// The height of the node.
    pub height: u32,
    /// The left child of the node.
    pub left: Option<Box<AVLTreeNode>>,
    /// The right child of the node.
    pub right: Option<Box<AVLTreeNode>>,
}

// Implementation of `AVLTreeNode`.
impl AVLTreeNode {
    /// Creating a new `AVLTreeNode` given the `key` and `value`.
    /// # Arguments
    /// * `key` - The key for the node.
    /// * `value` - The value for the node.
    pub fn new(key: i64, value: i64) -> Self {
        AVLTreeNode {
            key,
            value,
            height: 1,
            left: None,
            right: None,
        }
    }

    /// Function to return the balance factor of an `AVLTreeNode`.
    /// # Arguments
    /// * `self` - A ref to the `AVLTreeNode` in question.
    pub fn balance_factor(&self) -> i8 {
        let left_height = self.left.as_ref().map_or(0, |x| x.height);
        let right_height = self.right.as_ref().map_or(0, |x| x.height);
        (left_height as i64 - right_height as i64) as i8
    }

    /// Function to update the height of an `AVLTreeNode` after it has been moved.
    /// # Arguments
    /// * `self` - A mutable ref to the `AVLTreeNode` who's height is being updated.
    pub fn update_height(&mut self) {
        self.height = 1 + self
            .left
            .as_ref()
            .map_or(0, |x| x.height)
            .max(self.right.as_ref().map_or(0, |x| x.height));
    }
}

#[cfg(test)]
mod tests {
    use super::AVLTreeNode;

    #[test]
    fn test_balance_factor_no_children() {
        let test_node = AVLTreeNode::new(1, 1);
        assert_eq!(test_node.balance_factor(), 0)
    }

    #[test]
    fn test_balance_factor_left_child() {
        let mut test_node = AVLTreeNode::new(1, 1);
        test_node.left = Some(Box::new(AVLTreeNode::new(2, 2)));

        assert_eq!(test_node.balance_factor(), 1)
    }

    #[test]
    fn test_balance_factor_right_child() {
        let mut test_node = AVLTreeNode::new(1, 1);
        test_node.right = Some(Box::new(AVLTreeNode::new(2, 2)));

        assert_eq!(test_node.balance_factor(), -1)
    }

    #[test]
    fn test_balance_factor_2_children() {
        let mut test_node = AVLTreeNode::new(1, 1);
        test_node.right = Some(Box::new(AVLTreeNode::new(2, 2)));
        test_node.left = Some(Box::new(AVLTreeNode::new(2, 2)));

        assert_eq!(test_node.balance_factor(), 0)
    }

    #[test]
    fn test_update_height() {
        let mut test_node = AVLTreeNode::new(1, 1);
        test_node.right = Some(Box::new(AVLTreeNode::new(1, 2)));
        test_node.update_height();

        assert_eq!(test_node.height, 2);

        test_node.left = Some(Box::new(AVLTreeNode::new(1, 2)));
        test_node.update_height();

        assert_eq!(test_node.height, 2);
    }
}
