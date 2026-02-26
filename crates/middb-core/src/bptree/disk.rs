use super::buffer_pool::BufferPool;
use super::page::*;
use crate::storage::file::FileStorage;
use crate::storage::page::{Page, PAGE_SIZE};
use crate::{Error, Result};
use std::path::Path;

const META_PAGE_ID: u32 = 0;
const META_MAGIC: u64 = 0x4D49444242505452; // "MIDBBPTR"
const MAX_POOL_FRAMES: usize = 4096;

struct MetaPage {
    root_page_id: u32,
    entry_count: u64,
    height: u32,
    free_list_head: u32,
    next_page_id: u32,
}

pub struct DiskBPTree {
    pool: BufferPool,
    root_page_id: u32,
    entry_count: u64,
    height: u32,
    free_list_head: u32,
    next_page_id: u32,
}

impl DiskBPTree {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let storage = FileStorage::create_or_open(path)?;
        let mut pool = BufferPool::new(storage, MAX_POOL_FRAMES);

        let meta_id = pool.new_page()?;
        assert_eq!(meta_id, META_PAGE_ID);
        pool.unpin(meta_id);

        let root_id = pool.new_page()?;
        let root_page = encode_leaf_page(&LeafPageData {
            page_id: root_id,
            next_leaf: 0,
            prev_leaf: 0,
            entries: vec![],
        })?;
        pool.write_page(root_id, root_page)?;

        let mut tree = DiskBPTree {
            pool,
            root_page_id: root_id,
            entry_count: 0,
            height: 1,
            free_list_head: 0,
            next_page_id: root_id + 1,

        };
        tree.write_meta()?;
        tree.pool.flush_all()?;
        Ok(tree)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let storage = FileStorage::create_or_open(path)?;
        let mut pool = BufferPool::new(storage, MAX_POOL_FRAMES);

        let meta_page = pool.fetch_page(META_PAGE_ID)?;
        let meta = Self::read_meta_from_page(meta_page)?;
        pool.unpin(META_PAGE_ID);

        Ok(DiskBPTree {
            pool,
            root_page_id: meta.root_page_id,
            entry_count: meta.entry_count,
            height: meta.height,
            free_list_head: meta.free_list_head,
            next_page_id: meta.next_page_id,

        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.search(self.root_page_id, key, self.height)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let result = self.insert_recursive(self.root_page_id, key, value, self.height)?;

        if let Some((split_key, new_child_id)) = result.split {
            let new_root_id = self.alloc_page()?;
            let new_root = encode_interior_page(&InteriorPageData {
                page_id: new_root_id,
                keys: vec![split_key],
                children: vec![self.root_page_id, new_child_id],
            })?;
            self.pool.write_page(new_root_id, new_root)?;
            self.root_page_id = new_root_id;
            self.height += 1;
        }

        if result.is_new {
            self.entry_count += 1;
        }
        self.write_meta()?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let removed = self.delete_recursive(self.root_page_id, key, self.height)?;
        if removed {
            self.entry_count -= 1;
            self.write_meta()?;
        }
        Ok(removed)
    }

    pub fn range(&mut self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let leaf_id = self.find_leaf(self.root_page_id, start, self.height)?;
        let mut results = Vec::new();
        let mut current_leaf = leaf_id;

        loop {
            let page = self.pool.fetch_page(current_leaf)?.clone();
            self.pool.unpin(current_leaf);
            let leaf_data = decode_leaf_page(&page)?;

            for (k, v) in &leaf_data.entries {
                if k.as_slice() >= start && k.as_slice() < end {
                    results.push((k.clone(), v.clone()));
                } else if k.as_slice() >= end {
                    return Ok(results);
                }
            }

            if leaf_data.next_leaf == 0 {
                break;
            }
            current_leaf = leaf_data.next_leaf;
        }

        Ok(results)
    }

    pub fn len(&self) -> u64 {
        self.entry_count
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn height(&self) -> u32 {
        self.height
    }

    pub fn flush(&mut self) -> Result<()> {
        self.write_meta()?;
        self.pool.flush_all()
    }

    fn search(&mut self, page_id: u32, key: &[u8], height: u32) -> Result<Option<Vec<u8>>> {
        let page = self.pool.fetch_page(page_id)?.clone();
        self.pool.unpin(page_id);

        if height == 1 {
            let leaf = decode_leaf_page(&page)?;
            for (k, v) in &leaf.entries {
                if k.as_slice() == key {
                    return Ok(Some(v.clone()));
                }
            }
            return Ok(None);
        }

        let interior = decode_interior_page(&page)?;
        let child_idx = match interior.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        let child_id = interior.children[child_idx];
        self.search(child_id, key, height - 1)
    }

    fn insert_recursive(
        &mut self,
        page_id: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        height: u32,
    ) -> Result<InsertResult> {
        let page = self.pool.fetch_page(page_id)?.clone();
        self.pool.unpin(page_id);

        if height == 1 {
            return self.insert_into_leaf(page_id, &page, key, value);
        }

        let interior = decode_interior_page(&page)?;
        let child_idx = match interior.keys.binary_search_by(|k| k.as_slice().cmp(&key)) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        let child_id = interior.children[child_idx];
        let result = self.insert_recursive(child_id, key, value, height - 1)?;

        if let Some((split_key, new_child_id)) = result.split {
            return self.insert_into_interior(page_id, &page, split_key, new_child_id, result.is_new);
        }

        Ok(result)
    }

    fn insert_into_leaf(
        &mut self,
        page_id: u32,
        page: &Page,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<InsertResult> {
        let mut leaf = decode_leaf_page(page)?;

        let pos = leaf
            .entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(&key));

        let is_new = match pos {
            Ok(i) => {
                leaf.entries[i].1 = value;
                false
            }
            Err(i) => {
                leaf.entries.insert(i, (key, value));
                true
            }
        };

        if let Ok(encoded) = encode_leaf_page(&leaf) {
            self.pool.write_page(page_id, encoded)?;
            return Ok(InsertResult {
                split: None,
                is_new,
            });
        }

        let mid = leaf.entries.len() / 2;
        let right_entries: Vec<_> = leaf.entries.drain(mid..).collect();
        let split_key = right_entries[0].0.clone();

        let new_leaf_id = self.alloc_page()?;

        let old_next = leaf.next_leaf;
        leaf.next_leaf = new_leaf_id;
        let left_page = encode_leaf_page(&leaf)?;
        self.pool.write_page(page_id, left_page)?;

        let new_leaf = LeafPageData {
            page_id: new_leaf_id,
            next_leaf: old_next,
            prev_leaf: page_id,
            entries: right_entries,
        };
        let right_page = encode_leaf_page(&new_leaf)?;
        self.pool.write_page(new_leaf_id, right_page)?;

        if old_next != 0 {
            let next_page = self.pool.fetch_page(old_next)?.clone();
            self.pool.unpin(old_next);
            let mut next_leaf = decode_leaf_page(&next_page)?;
            next_leaf.prev_leaf = new_leaf_id;
            let next_encoded = encode_leaf_page(&next_leaf)?;
            self.pool.write_page(old_next, next_encoded)?;
        }

        Ok(InsertResult {
            split: Some((split_key, new_leaf_id)),
            is_new,
        })
    }

    fn insert_into_interior(
        &mut self,
        page_id: u32,
        page: &Page,
        split_key: Vec<u8>,
        new_child_id: u32,
        is_new: bool,
    ) -> Result<InsertResult> {
        let mut interior = decode_interior_page(page)?;
        let pos = match interior
            .keys
            .binary_search_by(|k| k.as_slice().cmp(&split_key))
        {
            Ok(i) => i + 1,
            Err(i) => i,
        };

        interior.keys.insert(pos, split_key);
        interior.children.insert(pos + 1, new_child_id);

        let max_keys = self.max_interior_keys();
        if interior.keys.len() <= max_keys {
            let encoded = encode_interior_page(&interior)?;
            self.pool.write_page(page_id, encoded)?;
            return Ok(InsertResult {
                split: None,
                is_new,
            });
        }

        let mid = interior.keys.len() / 2;
        let promote_key = interior.keys[mid].clone();

        let right_keys: Vec<_> = interior.keys.drain(mid + 1..).collect();
        interior.keys.pop();
        let right_children: Vec<_> = interior.children.drain(mid + 1..).collect();

        let new_interior_id = self.alloc_page()?;

        let left_page = encode_interior_page(&InteriorPageData {
            page_id,
            keys: interior.keys,
            children: interior.children,
        })?;
        self.pool.write_page(page_id, left_page)?;

        let right_page = encode_interior_page(&InteriorPageData {
            page_id: new_interior_id,
            keys: right_keys,
            children: right_children,
        })?;
        self.pool.write_page(new_interior_id, right_page)?;

        Ok(InsertResult {
            split: Some((promote_key, new_interior_id)),
            is_new,
        })
    }

    fn delete_recursive(&mut self, page_id: u32, key: &[u8], height: u32) -> Result<bool> {
        let page = self.pool.fetch_page(page_id)?.clone();
        self.pool.unpin(page_id);

        if height == 1 {
            let mut leaf = decode_leaf_page(&page)?;
            let pos = leaf
                .entries
                .binary_search_by(|(k, _)| k.as_slice().cmp(key));
            match pos {
                Ok(i) => {
                    leaf.entries.remove(i);
                    let encoded = encode_leaf_page(&leaf)?;
                    self.pool.write_page(page_id, encoded)?;
                    Ok(true)
                }
                Err(_) => Ok(false),
            }
        } else {
            let interior = decode_interior_page(&page)?;
            let child_idx = match interior.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(i) => i + 1,
                Err(i) => i,
            };
            let child_id = interior.children[child_idx];
            self.delete_recursive(child_id, key, height - 1)
        }
    }

    fn find_leaf(&mut self, page_id: u32, key: &[u8], height: u32) -> Result<u32> {
        if height == 1 {
            return Ok(page_id);
        }
        let page = self.pool.fetch_page(page_id)?.clone();
        self.pool.unpin(page_id);
        let interior = decode_interior_page(&page)?;
        let child_idx = match interior.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        self.find_leaf(interior.children[child_idx], key, height - 1)
    }

    fn alloc_page(&mut self) -> Result<u32> {
        if self.free_list_head != 0 {
            let page_id = self.free_list_head;
            let page = self.pool.fetch_page(page_id)?.clone();
            self.pool.unpin(page_id);
            self.free_list_head = u32::from_le_bytes(page.data()[0..4].try_into().unwrap());
            return Ok(page_id);
        }
        let id = self.pool.new_page()?;
        self.pool.unpin(id);
        if id >= self.next_page_id {
            self.next_page_id = id + 1;
        }
        Ok(id)
    }

    fn max_interior_keys(&self) -> usize {
        (PAGE_SIZE - 64) / 20
    }

    fn write_meta(&mut self) -> Result<()> {
        let mut page = Page::new();
        let buf = page.data_mut();
        buf[0..8].copy_from_slice(&META_MAGIC.to_le_bytes());
        buf[8..12].copy_from_slice(&self.root_page_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[20..24].copy_from_slice(&self.height.to_le_bytes());
        buf[24..28].copy_from_slice(&self.free_list_head.to_le_bytes());
        buf[28..32].copy_from_slice(&self.next_page_id.to_le_bytes());
        self.pool.write_page(META_PAGE_ID, page)?;
        Ok(())
    }

    fn read_meta_from_page(page: &Page) -> Result<MetaPage> {
        let buf = page.data();
        let magic = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        if magic != META_MAGIC {
            return Err(Error::Corruption(format!(
                "Invalid B+Tree meta page magic: {:#x}",
                magic
            )));
        }
        Ok(MetaPage {
            root_page_id: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            entry_count: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            height: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            free_list_head: u32::from_le_bytes(buf[24..28].try_into().unwrap()),
            next_page_id: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
        })
    }
}

struct InsertResult {
    split: Option<(Vec<u8>, u32)>,
    is_new: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_tree() -> (DiskBPTree, tempfile::NamedTempFile) {
        let temp = NamedTempFile::new().unwrap();
        let tree = DiskBPTree::create(temp.path()).unwrap();
        (tree, temp)
    }

    #[test]
    fn test_create_and_open() {
        let (mut tree, temp) = create_tree();
        tree.insert(b"key1".to_vec(), b"val1".to_vec()).unwrap();
        tree.flush().unwrap();
        drop(tree);

        let mut tree = DiskBPTree::open(temp.path()).unwrap();
        assert_eq!(tree.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_insert_get_delete() {
        let (mut tree, _temp) = create_tree();

        tree.insert(b"alpha".to_vec(), b"1".to_vec()).unwrap();
        tree.insert(b"beta".to_vec(), b"2".to_vec()).unwrap();
        tree.insert(b"gamma".to_vec(), b"3".to_vec()).unwrap();

        assert_eq!(tree.get(b"alpha").unwrap(), Some(b"1".to_vec()));
        assert_eq!(tree.get(b"beta").unwrap(), Some(b"2".to_vec()));
        assert_eq!(tree.get(b"gamma").unwrap(), Some(b"3".to_vec()));
        assert_eq!(tree.get(b"delta").unwrap(), None);
        assert_eq!(tree.len(), 3);

        assert!(tree.delete(b"beta").unwrap());
        assert_eq!(tree.get(b"beta").unwrap(), None);
        assert_eq!(tree.len(), 2);

        assert!(!tree.delete(b"nonexistent").unwrap());
    }

    #[test]
    fn test_update_existing_key() {
        let (mut tree, _temp) = create_tree();
        tree.insert(b"key".to_vec(), b"v1".to_vec()).unwrap();
        tree.insert(b"key".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(tree.get(b"key").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_many_inserts_cause_splits() {
        let (mut tree, _temp) = create_tree();
        for i in 0..500u32 {
            let key = format!("key{:05}", i);
            let val = format!("val{:05}", i);
            tree.insert(key.into_bytes(), val.into_bytes()).unwrap();
        }
        assert_eq!(tree.len(), 500);
        assert!(tree.height() > 1);

        for i in 0..500u32 {
            let key = format!("key{:05}", i);
            let val = format!("val{:05}", i);
            assert_eq!(
                tree.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes()),
                "Missing key {}",
                key
            );
        }
    }

    #[test]
    fn test_range_scan() {
        let (mut tree, _temp) = create_tree();
        for i in 0..100u32 {
            let key = format!("k{:04}", i);
            let val = format!("v{:04}", i);
            tree.insert(key.into_bytes(), val.into_bytes()).unwrap();
        }

        let results = tree.range(b"k0020", b"k0030").unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(results[0].0, b"k0020");
        assert_eq!(results[9].0, b"k0029");
    }

    #[test]
    fn test_persistence_after_flush() {
        let temp = NamedTempFile::new().unwrap();
        {
            let mut tree = DiskBPTree::create(temp.path()).unwrap();
            for i in 0..200u32 {
                let key = format!("pk{:04}", i);
                tree.insert(key.into_bytes(), vec![i as u8; 32]).unwrap();
            }
            tree.flush().unwrap();
        }
        {
            let mut tree = DiskBPTree::open(temp.path()).unwrap();
            assert_eq!(tree.len(), 200);
            for i in 0..200u32 {
                let key = format!("pk{:04}", i);
                assert!(tree.get(key.as_bytes()).unwrap().is_some());
            }
        }
    }

    #[test]
    fn test_empty_tree() {
        let (mut tree, _temp) = create_tree();
        assert!(tree.is_empty());
        assert_eq!(tree.get(b"anything").unwrap(), None);
        assert!(!tree.delete(b"anything").unwrap());
        assert!(tree.range(b"a", b"z").unwrap().is_empty());
    }
}
