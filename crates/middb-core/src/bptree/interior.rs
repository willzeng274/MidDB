use super::NodePtr;
use std::cell::RefCell;

pub struct InteriorNode<const FANOUT: usize, K, V> {
    pub(super) keys: RefCell<Vec<K>>,
    pub(super) children: RefCell<Vec<NodePtr<FANOUT, K, V>>>,
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> InteriorNode<FANOUT, K, V> {
    pub fn new() -> Self {
        InteriorNode {
            keys: RefCell::new(Vec::new()),
            children: RefCell::new(Vec::new()),
        }
    }

    pub fn search(&self, key: &K) -> Option<V> {
        let keys = self.keys.borrow();
        let children = self.children.borrow();

        let idx = match keys.binary_search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        children.get(idx)?.search(key)
    }

    pub fn insert_child(&self, key: K, child: NodePtr<FANOUT, K, V>) {
        let mut keys = self.keys.borrow_mut();
        let mut children = self.children.borrow_mut();

        let idx = match keys.binary_search(&key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        keys.insert(idx, key);
        children.insert(idx + 1, child);
    }

    pub fn is_full(&self) -> bool {
        self.keys.borrow().len() >= FANOUT
    }

    pub fn split(&self) -> (K, NodePtr<FANOUT, K, V>) {
        let mut keys = self.keys.borrow_mut();
        let mut children = self.children.borrow_mut();

        let mid = keys.len() / 2;
        let middle_key = keys.remove(mid);
        let new_keys = keys.split_off(mid);
        let new_children = children.split_off(mid + 1);

        let new_interior = InteriorNode {
            keys: RefCell::new(new_keys),
            children: RefCell::new(new_children),
        };

        let new_interior_ptr = std::sync::Arc::new(super::Node::Interior(new_interior));
        (middle_key, new_interior_ptr)
    }

    pub fn get_child(&self, key: &K) -> Option<NodePtr<FANOUT, K, V>> {
        let keys = self.keys.borrow();
        let children = self.children.borrow();

        let idx = match keys.binary_search(key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        children.get(idx).cloned()
    }
}
