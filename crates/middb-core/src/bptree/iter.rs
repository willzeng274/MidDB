use super::{Node, NodePtr};
use std::sync::Arc;

pub struct BPTreeIter<const FANOUT: usize, K, V> {
    current_leaf: Option<NodePtr<FANOUT, K, V>>,
    current_idx: usize,
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> BPTreeIter<FANOUT, K, V> {
    pub fn new(root: &NodePtr<FANOUT, K, V>) -> Self {
        let current_leaf = Self::find_leftmost_leaf(root);
        BPTreeIter {
            current_leaf,
            current_idx: 0,
        }
    }

    fn find_leftmost_leaf(node: &NodePtr<FANOUT, K, V>) -> Option<NodePtr<FANOUT, K, V>> {
        let mut current = Arc::clone(node);
        loop {
            match current.as_ref() {
                Node::Leaf(_) => return Some(current),
                Node::Interior(interior) => {
                    let children = interior.children.borrow();
                    let first = Arc::clone(children.first()?);
                    drop(children);
                    current = first;
                }
            }
        }
    }
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> Iterator for BPTreeIter<FANOUT, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let leaf_node = self.current_leaf.as_ref()?;
        let leaf = leaf_node.as_leaf()?;

        let keys = leaf.keys.borrow();
        let values = leaf.values.borrow();

        if self.current_idx < keys.len() {
            let result = (keys[self.current_idx].clone(), values[self.current_idx].clone());
            self.current_idx += 1;
            drop(keys);
            drop(values);
            Some(result)
        } else {
            drop(keys);
            drop(values);

            if let Some(next_node) = leaf.get_next().upgrade() {
                self.current_leaf = Some(next_node);
                self.current_idx = 0;
                self.next()
            } else {
                None
            }
        }
    }
}

pub struct RangeIter<const FANOUT: usize, K, V> {
    current_leaf: Option<NodePtr<FANOUT, K, V>>,
    current_idx: usize,
    end: K,
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> RangeIter<FANOUT, K, V> {
    pub fn new(root: &NodePtr<FANOUT, K, V>, start: &K, end: &K) -> Self {
        let (current_leaf, start_idx) = Self::find_start_position(root, start);
        RangeIter {
            current_leaf,
            current_idx: start_idx,
            end: end.clone(),
        }
    }

    fn find_start_position(node: &NodePtr<FANOUT, K, V>, start: &K) -> (Option<NodePtr<FANOUT, K, V>>, usize) {
        let mut current = Arc::clone(node);
        loop {
            match current.as_ref() {
                Node::Leaf(leaf) => {
                    let keys = leaf.keys.borrow();
                    let idx = match keys.binary_search(start) {
                        Ok(i) => i,
                        Err(i) => i,
                    };
                    drop(keys);
                    return (Some(current), idx);
                }
                Node::Interior(interior) => {
                    let keys = interior.keys.borrow();
                    let idx = match keys.binary_search(start) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    drop(keys);

                    let children = interior.children.borrow();
                    if let Some(child) = children.get(idx) {
                        let next = Arc::clone(child);
                        drop(children);
                        current = next;
                    } else {
                        return (None, 0);
                    }
                }
            }
        }
    }
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> Iterator for RangeIter<FANOUT, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_node = self.current_leaf.as_ref()?;
            let leaf = leaf_node.as_leaf()?;

            let keys = leaf.keys.borrow();
            let values = leaf.values.borrow();

            if self.current_idx < keys.len() {
                let key = &keys[self.current_idx];
                if key >= &self.end {
                    return None;
                }
                let result = (key.clone(), values[self.current_idx].clone());
                self.current_idx += 1;
                drop(keys);
                drop(values);
                return Some(result);
            }

            drop(keys);
            drop(values);

            if let Some(next_node) = leaf.get_next().upgrade() {
                self.current_leaf = Some(next_node);
                self.current_idx = 0;
                continue;
            }

            return None;
        }
    }
}
