use crate::storage::file::FileStorage;
use crate::storage::page::Page;
use crate::{Error, PageId, Result};
use std::collections::HashMap;

const DEFAULT_POOL_SIZE: usize = 1024;

struct FrameEntry {
    page: Page,
    dirty: bool,
    pin_count: u32,
    lru_counter: u64,
}

pub struct BufferPool {
    storage: FileStorage,
    frames: HashMap<u32, FrameEntry>,
    max_frames: usize,
    lru_clock: u64,
}

impl BufferPool {
    pub fn new(storage: FileStorage, max_frames: usize) -> Self {
        BufferPool {
            storage,
            frames: HashMap::with_capacity(max_frames),
            max_frames: if max_frames == 0 {
                DEFAULT_POOL_SIZE
            } else {
                max_frames
            },
            lru_clock: 0,
        }
    }

    pub fn fetch_page(&mut self, page_id: u32) -> Result<&Page> {
        if !self.frames.contains_key(&page_id) {
            self.load_page(page_id)?;
        }
        let frame = self.frames.get_mut(&page_id).unwrap();
        self.lru_clock += 1;
        frame.lru_counter = self.lru_clock;
        frame.pin_count += 1;
        Ok(&self.frames[&page_id].page)
    }

    pub fn fetch_page_mut(&mut self, page_id: u32) -> Result<&mut Page> {
        if !self.frames.contains_key(&page_id) {
            self.load_page(page_id)?;
        }
        let frame = self.frames.get_mut(&page_id).unwrap();
        self.lru_clock += 1;
        frame.lru_counter = self.lru_clock;
        frame.pin_count += 1;
        frame.dirty = true;
        Ok(&mut self.frames.get_mut(&page_id).unwrap().page)
    }

    pub fn new_page(&mut self) -> Result<u32> {
        let page_id = self.storage.allocate_page()? as u32;
        if self.frames.len() >= self.max_frames {
            self.evict_one()?;
        }
        self.lru_clock += 1;
        self.frames.insert(
            page_id,
            FrameEntry {
                page: Page::new(),
                dirty: true,
                pin_count: 1,
                lru_counter: self.lru_clock,
            },
        );
        Ok(page_id)
    }

    pub fn mark_dirty(&mut self, page_id: u32) {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            frame.dirty = true;
        }
    }

    pub fn unpin(&mut self, page_id: u32) {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
    }

    pub fn write_page(&mut self, page_id: u32, page: Page) -> Result<()> {
        if self.frames.len() >= self.max_frames && !self.frames.contains_key(&page_id) {
            self.evict_one()?;
        }
        self.lru_clock += 1;
        self.frames.insert(
            page_id,
            FrameEntry {
                page,
                dirty: true,
                pin_count: 0,
                lru_counter: self.lru_clock,
            },
        );
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        let dirty_pages: Vec<(u32, Page)> = self
            .frames
            .iter()
            .filter(|(_, f)| f.dirty)
            .map(|(&id, f)| (id, f.page.clone()))
            .collect();

        for (page_id, page) in dirty_pages {
            self.storage.write_page(page_id as PageId, &page)?;
            if let Some(frame) = self.frames.get_mut(&page_id) {
                frame.dirty = false;
            }
        }
        self.storage.sync()?;
        Ok(())
    }

    pub fn flush_page(&mut self, page_id: u32) -> Result<()> {
        if let Some(frame) = self.frames.get(&page_id) {
            if frame.dirty {
                let page = frame.page.clone();
                self.storage.write_page(page_id as PageId, &page)?;
                self.frames.get_mut(&page_id).unwrap().dirty = false;
            }
        }
        Ok(())
    }

    fn load_page(&mut self, page_id: u32) -> Result<()> {
        if self.frames.len() >= self.max_frames {
            self.evict_one()?;
        }
        let page = self.storage.read_page(page_id as PageId)?;
        self.lru_clock += 1;
        self.frames.insert(
            page_id,
            FrameEntry {
                page,
                dirty: false,
                pin_count: 0,
                lru_counter: self.lru_clock,
            },
        );
        Ok(())
    }

    fn evict_one(&mut self) -> Result<()> {
        let victim = self
            .frames
            .iter()
            .filter(|(_, f)| f.pin_count == 0)
            .min_by_key(|(_, f)| f.lru_counter)
            .map(|(&id, _)| id);

        let victim_id = victim.ok_or_else(|| {
            Error::Internal("Buffer pool full: all pages pinned".into())
        })?;

        if let Some(frame) = self.frames.get(&victim_id) {
            if frame.dirty {
                let page = frame.page.clone();
                self.storage
                    .write_page(victim_id as PageId, &page)?;
            }
        }
        self.frames.remove(&victim_id);
        Ok(())
    }

    pub fn storage(&self) -> &FileStorage {
        &self.storage
    }

    pub fn storage_mut(&mut self) -> &mut FileStorage {
        &mut self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_pool(max_frames: usize) -> BufferPool {
        let temp = NamedTempFile::new().unwrap();
        let storage = FileStorage::create_or_open(temp.path()).unwrap();
        BufferPool::new(storage, max_frames)
    }

    #[test]
    fn test_new_page_and_fetch() {
        let mut pool = create_pool(16);
        let pid = pool.new_page().unwrap();
        pool.unpin(pid);

        let page = pool.fetch_page(pid).unwrap();
        assert_eq!(page.data().len(), 4096);
        pool.unpin(pid);
    }

    #[test]
    fn test_dirty_tracking_and_flush() {
        let mut pool = create_pool(16);
        let pid = pool.new_page().unwrap();
        pool.unpin(pid);

        let page = pool.fetch_page_mut(pid).unwrap();
        page.write_at(0, b"hello").unwrap();
        pool.unpin(pid);

        pool.flush_all().unwrap();

        let page = pool.fetch_page(pid).unwrap();
        assert_eq!(&page.data()[..5], b"hello");
        pool.unpin(pid);
    }

    #[test]
    fn test_eviction() {
        let mut pool = create_pool(4);
        let mut ids = vec![];
        for _ in 0..4 {
            let pid = pool.new_page().unwrap();
            pool.unpin(pid);
            ids.push(pid);
        }

        let extra = pool.new_page().unwrap();
        pool.unpin(extra);
        assert!(pool.frames.len() <= 4);
    }

    #[test]
    fn test_eviction_pinned_pages_protected() {
        let mut pool = create_pool(2);
        let p1 = pool.new_page().unwrap();
        let p2 = pool.new_page().unwrap();
        pool.unpin(p2);

        // p1 is still pinned, p2 should be evicted
        let p3 = pool.new_page().unwrap();
        pool.unpin(p3);
        assert!(pool.frames.contains_key(&p1));
    }
}
