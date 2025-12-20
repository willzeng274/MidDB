use super::{LeafNode, InteriorNode};
use std::sync::Arc;

pub type NodePtr<const FANOUT: usize, K, V> = Arc<Node<FANOUT, K, V>>;
pub type NodeWeakPtr<const FANOUT: usize, K, V> = std::sync::Weak<Node<FANOUT, K, V>>;

pub enum Node<const FANOUT: usize, K, V> {
    Leaf(LeafNode<FANOUT, K, V>),
    Interior(InteriorNode<FANOUT, K, V>),
}

impl<const FANOUT: usize, K: Ord + Clone, V: Clone> Node<FANOUT, K, V> {
    pub fn new_leaf() -> NodePtr<FANOUT, K, V> {
        Arc::new(Node::Leaf(LeafNode::new()))
    }

    pub fn new_interior() -> NodePtr<FANOUT, K, V> {
        Arc::new(Node::Interior(InteriorNode::new()))
    }

    pub fn search(&self, key: &K) -> Option<V> {
        match self {
            Node::Leaf(leaf) => leaf.search(key),
            Node::Interior(interior) => interior.search(key),
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }

    pub fn as_leaf(&self) -> Option<&LeafNode<FANOUT, K, V>> {
        match self {
            Node::Leaf(leaf) => Some(leaf),
            _ => None,
        }
    }

    pub fn as_interior(&self) -> Option<&InteriorNode<FANOUT, K, V>> {
        match self {
            Node::Interior(interior) => Some(interior),
            _ => None,
        }
    }
}
