mod node;
mod leaf;
mod interior;
mod iter;

pub use node::{Node, NodePtr, NodeWeakPtr};
pub use leaf::LeafNode;
pub use interior::InteriorNode;
pub use iter::{BPTreeIter, RangeIter};

use std::sync::Arc;

pub struct BPTree<const FANOUT: usize, K, V> {
    root: NodePtr<FANOUT, K, V>,
    len: usize,
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> BPTree<FANOUT, K, V> {
    pub fn new() -> Self {
        assert!(FANOUT >= 3, "FANOUT must be at least 3");
        BPTree {
            root: Node::new_leaf(),
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.root.search(key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        let (split_result, is_new) = Self::insert_recursive(&self.root, key, value);
        if is_new {
            self.len += 1;
        }

        if let Some((split_key, new_child)) = split_result {
            let new_root = Node::new_interior();
            if let Some(interior) = new_root.as_interior() {
                interior.children.borrow_mut().push(Arc::clone(&self.root));
                interior.children.borrow_mut().push(new_child);
                interior.keys.borrow_mut().push(split_key);
            }
            self.root = new_root;
        }
    }

    fn insert_recursive(
        node: &NodePtr<FANOUT, K, V>,
        key: K,
        value: V,
    ) -> (Option<(K, NodePtr<FANOUT, K, V>)>, bool) {
        match node.as_ref() {
            Node::Leaf(leaf) => {
                let is_new = leaf.insert(key, value).is_none();

                if leaf.is_full() {
                    let (split_key, new_leaf_node) = leaf.split(node);
                    (Some((split_key, new_leaf_node)), is_new)
                } else {
                    (None, is_new)
                }
            }
            Node::Interior(interior) => {
                let keys = interior.keys.borrow();
                let idx = match keys.binary_search(&key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                drop(keys);

                let child = {
                    let children = interior.children.borrow();
                    Arc::clone(&children[idx])
                };

                let (split_result, is_new) = Self::insert_recursive(&child, key, value);

                if let Some((split_key, new_child)) = split_result {
                    interior.insert_child(split_key, new_child);

                    if interior.is_full() {
                        let (new_split_key, new_interior) = interior.split();
                        return (Some((new_split_key, new_interior)), is_new);
                    }
                }

                (None, is_new)
            }
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let result = Self::remove_recursive(&self.root, key);
        if result.is_some() {
            self.len -= 1;
        }
        result
    }

    fn remove_recursive(node: &NodePtr<FANOUT, K, V>, key: &K) -> Option<V> {
        match node.as_ref() {
            Node::Leaf(leaf) => leaf.remove(key),
            Node::Interior(interior) => {
                let child = interior.get_child(key)?;
                Self::remove_recursive(&child, key)
            }
        }
    }

    pub fn iter(&self) -> BPTreeIter<FANOUT, K, V> {
        BPTreeIter::new(&self.root)
    }

    pub fn range(&self, start: &K, end: &K) -> RangeIter<FANOUT, K, V> {
        RangeIter::new(&self.root, start, end)
    }
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> Default for BPTree<FANOUT, K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let tree: BPTree<4, i32, i32> = BPTree::new();
        assert_eq!(tree.len(), 0);
        assert!(tree.is_empty());
    }

    #[test]
    fn test_insert_and_get() {
        let mut tree = BPTree::<4, _, _>::new();
        tree.insert(1, "one");
        tree.insert(2, "two");
        tree.insert(3, "three");

        assert_eq!(tree.get(&1), Some("one"));
        assert_eq!(tree.get(&2), Some("two"));
        assert_eq!(tree.get(&3), Some("three"));
        assert_eq!(tree.get(&4), None);
        assert_eq!(tree.len(), 3);
    }

    #[test]
    fn test_update() {
        let mut tree = BPTree::<4, _, _>::new();
        tree.insert(1, "one");
        tree.insert(1, "ONE");

        assert_eq!(tree.get(&1), Some("ONE"));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_split() {
        let mut tree = BPTree::<4, _, _>::new();
        for i in 0..10 {
            tree.insert(i, i * 10);
        }

        assert_eq!(tree.len(), 10);
        for i in 0..10 {
            assert_eq!(tree.get(&i), Some(i * 10));
        }
    }

    #[test]
    fn test_iter() {
        let mut tree = BPTree::<4, _, _>::new();
        tree.insert(3, 30);
        tree.insert(1, 10);
        tree.insert(2, 20);

        let items: Vec<_> = tree.iter().collect();
        assert_eq!(items, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[test]
    fn test_range() {
        let mut tree = BPTree::<4, _, _>::new();
        for i in 0..10 {
            tree.insert(i, i * 10);
        }

        let items: Vec<_> = tree.range(&3, &7).collect();
        assert_eq!(items, vec![(3, 30), (4, 40), (5, 50), (6, 60)]);
    }

    #[test]
    fn test_remove() {
        let mut tree = BPTree::<4, _, _>::new();
        tree.insert(1, 10);
        tree.insert(2, 20);
        tree.insert(3, 30);

        assert_eq!(tree.remove(&2), Some(20));
        assert_eq!(tree.len(), 2);
        assert_eq!(tree.get(&2), None);
        assert_eq!(tree.get(&1), Some(10));
    }
}
