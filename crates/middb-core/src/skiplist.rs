use std::cmp::Ordering;
use std::fmt;

const MAX_HEIGHT: usize = 16;
const P: f64 = 0.25;

struct Node<K, V> {
    key: K,
    value: V,
    forward: Vec<Option<Box<Node<K, V>>>>,
}

impl<K, V> Node<K, V> {
    fn new(key: K, value: V, height: usize) -> Self {
        let mut forward = Vec::with_capacity(height);
        for _ in 0..height {
            forward.push(None);
        }
        Node {
            key,
            value,
            forward,
        }
    }

    fn height(&self) -> usize {
        self.forward.len()
    }
}

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for Node<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("height", &self.height())
            .finish()
    }
}

pub struct SkipList<K, V> {
    head: Box<Node<K, V>>,
    len: usize,
    height: usize,
}

impl<K: Ord + Default, V: Default> SkipList<K, V> {
    pub fn new() -> Self {
        SkipList {
            head: Box::new(Node::new(K::default(), V::default(), MAX_HEIGHT)),
            len: 0,
            height: 1,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn random_height() -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && rand() < P {
            height += 1;
        }
        height
    }

    fn find_update_path(&mut self, key: &K) -> [*mut Node<K, V>; MAX_HEIGHT] {
        let mut update: [*mut Node<K, V>; MAX_HEIGHT] = [std::ptr::null_mut(); MAX_HEIGHT];
        let mut current = &mut *self.head as *mut Node<K, V>;

        unsafe {
            for level in (0..self.height).rev() {
                while let Some(ref next) = (&(*current).forward)[level] {
                    if next.key < *key {
                        current = (&mut (*current).forward)[level].as_mut().unwrap().as_mut() as *mut Node<K, V>;
                    } else {
                        break;
                    }
                }
                update[level] = current;
            }
        }

        update
    }

    pub fn insert(&mut self, key: K, value: V) {
        let mut update = self.find_update_path(&key);

        unsafe {
            let next_node = (&(*update[0]).forward)[0].as_ref();
            if let Some(node) = next_node {
                if node.key == key {
                    let node_ptr = (&mut (*update[0]).forward)[0].as_mut().unwrap().as_mut() as *mut Node<K, V>;
                    (*node_ptr).value = value;
                    return;
                }
            }

            let height = Self::random_height();
            let mut new_node = Box::new(Node::new(key, value, height));

            if height > self.height {
                for i in self.height..height {
                    update[i] = &mut *self.head as *mut Node<K, V>;
                }
                self.height = height;
            }

            for level in 0..height {
                new_node.forward[level] = (&mut (*update[level]).forward)[level].take();
                (&mut (*update[level]).forward)[level] = Some(new_node);
                new_node = (&mut (*update[level]).forward)[level].take().unwrap();
            }
            (&mut (*update[0]).forward)[0] = Some(new_node);

            self.len += 1;
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let mut current = &self.head;

        for level in (0..self.height).rev() {
            while let Some(ref next) = current.forward[level] {
                match next.key.cmp(key) {
                    Ordering::Less => current = next,
                    Ordering::Equal => return Some(&next.value),
                    Ordering::Greater => break,
                }
            }
        }

        None
    }

    pub fn iter(&self) -> SkipListIter<'_, K, V> {
        SkipListIter {
            current: self.head.forward[0].as_ref().map(|n| &**n),
        }
    }

    pub fn range<'a>(&'a self, start: &K, end: &'a K) -> RangeIter<'a, K, V> {
        let mut current = &self.head;

        for level in (0..self.height).rev() {
            while let Some(ref next) = current.forward[level] {
                if next.key < *start {
                    current = next;
                } else {
                    break;
                }
            }
        }

        let start_node = current.forward[0].as_ref().map(|n| &**n);

        RangeIter {
            current: start_node,
            end,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let mut update = self.find_update_path(key);

        unsafe {
            let next_node = (&(*update[0]).forward)[0].as_ref();
            if let Some(node) = next_node {
                if node.key == *key {
                    let mut removed = (&mut (*update[0]).forward)[0].take().unwrap();

                    for level in 0..removed.height() {
                        if update[level].is_null() {
                            update[level] = &mut *self.head as *mut Node<K, V>;
                        }
                        (&mut (*update[level]).forward)[level] = removed.forward[level].take();
                    }

                    while self.height > 1 && self.head.forward[self.height - 1].is_none() {
                        self.height -= 1;
                    }

                    self.len -= 1;
                    return Some(removed.value);
                }
            }
        }

        None
    }
}

impl<K: Ord + Default, V: Default> Default for SkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SkipListIter<'a, K, V> {
    current: Option<&'a Node<K, V>>,
}

impl<'a, K, V> Iterator for SkipListIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.map(|node| {
            self.current = node.forward[0].as_ref().map(|n| &**n);
            (&node.key, &node.value)
        })
    }
}

pub struct RangeIter<'a, K, V> {
    current: Option<&'a Node<K, V>>,
    end: &'a K,
}

impl<'a, K: Ord, V> Iterator for RangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.and_then(|node| {
            if node.key < *self.end {
                self.current = node.forward[0].as_ref().map(|n| &**n);
                Some((&node.key, &node.value))
            } else {
                None
            }
        })
    }
}

fn rand() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static SEED: Cell<u64> = Cell::new(12345);
    }

    SEED.with(|seed| {
        let s = seed.get();
        let next = s.wrapping_mul(1103515245).wrapping_add(12345);
        seed.set(next);
        ((next / 65536) % 32768) as f64 / 32768.0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let mut list = SkipList::new();
        list.insert(1, "one");
        list.insert(2, "two");
        list.insert(3, "three");

        assert_eq!(list.get(&1), Some(&"one"));
        assert_eq!(list.get(&2), Some(&"two"));
        assert_eq!(list.get(&3), Some(&"three"));
        assert_eq!(list.get(&4), None);
    }

    #[test]
    fn test_update_existing() {
        let mut list = SkipList::new();
        list.insert(1, "one");
        list.insert(1, "ONE");

        assert_eq!(list.get(&1), Some(&"ONE"));
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn test_len_and_empty() {
        let mut list = SkipList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);

        list.insert(1, "one");
        assert!(!list.is_empty());
        assert_eq!(list.len(), 1);

        list.insert(2, "two");
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_iterator() {
        let mut list = SkipList::new();
        list.insert(3, "three");
        list.insert(1, "one");
        list.insert(2, "two");

        let items: Vec<_> = list.iter().collect();
        assert_eq!(items, vec![(&1, &"one"), (&2, &"two"), (&3, &"three")]);
    }

    #[test]
    fn test_range() {
        let mut list = SkipList::new();
        for i in 0..10 {
            list.insert(i, i * 10);
        }

        let items: Vec<_> = list.range(&3, &7).map(|(k, v)| (*k, *v)).collect();
        assert_eq!(items, vec![(3, 30), (4, 40), (5, 50), (6, 60)]);
    }

    #[test]
    fn test_remove() {
        let mut list = SkipList::new();
        list.insert(1, "one");
        list.insert(2, "two");
        list.insert(3, "three");

        assert_eq!(list.remove(&2), Some("two"));
        assert_eq!(list.len(), 2);
        assert_eq!(list.get(&2), None);
        assert_eq!(list.get(&1), Some(&"one"));
        assert_eq!(list.get(&3), Some(&"three"));
    }

    #[test]
    fn test_empty_list() {
        let list: SkipList<i32, i32> = SkipList::new();
        assert_eq!(list.get(&1), None);
        assert_eq!(list.iter().count(), 0);
    }

    #[test]
    fn test_single_element() {
        let mut list = SkipList::new();
        list.insert(42, "answer");

        assert_eq!(list.get(&42), Some(&"answer"));
        assert_eq!(list.len(), 1);
        assert_eq!(list.iter().count(), 1);
    }

    #[cfg(test)]
    mod proptests {
        use super::*;
        use proptest::prelude::*;
        use std::collections::BTreeMap;

        proptest! {
            #[test]
            fn prop_insert_and_retrieve(keys in prop::collection::vec(0i32..10000, 0..1000)) {
                let mut list = SkipList::new();
                let mut expected = BTreeMap::new();

                for key in keys.iter() {
                    let value = key * 2;
                    list.insert(*key, value);
                    expected.insert(*key, value);
                }

                for (key, expected_value) in expected.iter() {
                    prop_assert_eq!(list.get(key), Some(expected_value));
                }

                prop_assert_eq!(list.get(&10001), None);
            }

            #[test]
            fn prop_sorted_iteration(keys in prop::collection::vec(0i32..1000, 0..500)) {
                let mut list = SkipList::new();
                let mut expected = BTreeMap::new();

                for key in keys {
                    list.insert(key, key * 2);
                    expected.insert(key, key * 2);
                }

                let list_items: Vec<_> = list.iter().map(|(k, v)| (*k, *v)).collect();
                let btree_items: Vec<_> = expected.iter().map(|(k, v)| (*k, *v)).collect();

                prop_assert_eq!(list_items, btree_items);
            }

            #[test]
            fn prop_len_matches_inserts(keys in prop::collection::vec(0i32..1000, 0..500)) {
                let mut list = SkipList::new();
                let mut unique_keys = std::collections::HashSet::new();

                for key in keys {
                    list.insert(key, key);
                    unique_keys.insert(key);
                }

                prop_assert_eq!(list.len(), unique_keys.len());
            }

            #[test]
            fn prop_remove_works(keys in prop::collection::vec(0i32..500, 10..100)) {
                let mut list = SkipList::new();
                let unique_keys: Vec<_> = {
                    let mut set = std::collections::HashSet::new();
                    for k in keys {
                        set.insert(k);
                    }
                    set.into_iter().collect()
                };

                for key in &unique_keys {
                    list.insert(*key, *key * 2);
                }

                for key in unique_keys.iter().take(unique_keys.len() / 2) {
                    let removed = list.remove(key);
                    prop_assert_eq!(removed, Some(*key * 2));
                }

                for key in unique_keys.iter().take(unique_keys.len() / 2) {
                    prop_assert_eq!(list.get(key), None);
                }

                for key in unique_keys.iter().skip(unique_keys.len() / 2) {
                    let expected_value = *key * 2;
                    prop_assert_eq!(list.get(key), Some(&expected_value));
                }
            }

            #[test]
            fn prop_range_query(keys in prop::collection::vec(0i32..1000, 0..500)) {
                let mut list = SkipList::new();
                let mut btree = BTreeMap::new();

                for key in keys {
                    list.insert(key, key * 2);
                    btree.insert(key, key * 2);
                }

                let start = 250;
                let end = 750;

                let list_range: Vec<_> = list.range(&start, &end).map(|(k, v)| (*k, *v)).collect();
                let btree_range: Vec<_> = btree.range(start..end).map(|(k, v)| (*k, *v)).collect();

                prop_assert_eq!(list_range, btree_range);
            }
        }
    }
}
