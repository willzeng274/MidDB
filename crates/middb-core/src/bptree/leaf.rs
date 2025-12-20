use super::{NodeWeakPtr};
use std::cell::RefCell;

pub struct LeafNode<const FANOUT: usize, K, V> {
    pub(super) keys: RefCell<Vec<K>>,
    pub(super) values: RefCell<Vec<V>>,
    #[allow(dead_code)]
    prev: RefCell<NodeWeakPtr<FANOUT, K, V>>,
    next: RefCell<NodeWeakPtr<FANOUT, K, V>>,
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> LeafNode<FANOUT, K, V> {
    pub fn new() -> Self {
        LeafNode {
            keys: RefCell::new(Vec::new()),
            values: RefCell::new(Vec::new()),
            prev: RefCell::new(std::sync::Weak::new()),
            next: RefCell::new(std::sync::Weak::new()),
        }
    }

    pub fn search(&self, key: &K) -> Option<V> {
        let keys = self.keys.borrow();
        keys.binary_search(key)
            .ok()
            .map(|idx| self.values.borrow()[idx].clone())
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let mut keys = self.keys.borrow_mut();
        let mut values = self.values.borrow_mut();

        match keys.binary_search(&key) {
            Ok(idx) => {
                let old = values[idx].clone();
                values[idx] = value;
                Some(old)
            }
            Err(idx) => {
                keys.insert(idx, key);
                values.insert(idx, value);
                None
            }
        }
    }

    pub fn is_full(&self) -> bool {
        self.keys.borrow().len() >= FANOUT
    }

    pub fn len(&self) -> usize {
        self.keys.borrow().len()
    }

    pub fn split(&self, leaf_ptr: &super::NodePtr<FANOUT, K, V>) -> (K, super::NodePtr<FANOUT, K, V>) {
        let mut keys = self.keys.borrow_mut();
        let mut values = self.values.borrow_mut();

        let mid = keys.len() / 2;
        let new_keys = keys.split_off(mid);
        let new_values = values.split_off(mid);
        let middle_key = new_keys[0].clone();

        let new_leaf = LeafNode {
            keys: RefCell::new(new_keys),
            values: RefCell::new(new_values),
            prev: RefCell::new(std::sync::Arc::downgrade(leaf_ptr)),
            next: RefCell::new(self.next.borrow().clone()),
        };

        let new_leaf_ptr = std::sync::Arc::new(super::Node::Leaf(new_leaf));
        *self.next.borrow_mut() = std::sync::Arc::downgrade(&new_leaf_ptr);

        (middle_key, new_leaf_ptr)
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let mut keys = self.keys.borrow_mut();
        match keys.binary_search(key) {
            Ok(idx) => {
                keys.remove(idx);
                let mut values = self.values.borrow_mut();
                Some(values.remove(idx))
            }
            Err(_) => None,
        }
    }

    pub fn get_next(&self) -> NodeWeakPtr<FANOUT, K, V> {
        self.next.borrow().clone()
    }
}
