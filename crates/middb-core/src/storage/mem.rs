use super::page::Page;
use crate::{PageId, Result};
use std::collections::HashMap;

pub struct MemStorage {
    pages: HashMap<PageId, Page>,
    next_page_id: PageId,
}

impl MemStorage {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        MemStorage {
            pages: HashMap::new(),
            next_page_id: 0,
        }
    }
    
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.pages
            .get(&page_id)
            .cloned()
            .ok_or_else(|| crate::Error::InvalidArgument(format!("Page {} not found", page_id)))
    }
    
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        self.pages.insert(page_id, page.clone());
        
        if page_id >= self.next_page_id {
            self.next_page_id = page_id + 1;
        }
        
        Ok(())
    }
    
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        
        let page = Page::new();
        self.write_page(page_id, &page)?;
        
        Ok(page_id)
    }
    
    pub fn free_page(&mut self, page_id: PageId) -> Result<()> {
        self.pages.remove(&page_id);
        Ok(())
    }
    
    pub fn num_pages(&self) -> usize {
        self.pages.len()
    }
    
    pub fn clear(&mut self) {
        self.pages.clear();
        self.next_page_id = 0;
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_mem_storage() {
        let mut storage = MemStorage::new();
        
        // Allocate pages
        let page0 = storage.allocate_page().unwrap();
        let page1 = storage.allocate_page().unwrap();
        
        assert_eq!(page0, 0);
        assert_eq!(page1, 1);
        
        // Write and read
        let mut page = Page::new();
        page.write_at(0, b"test").unwrap();
        storage.write_page(page0, &page).unwrap();
        
        let read_page = storage.read_page(page0).unwrap();
        assert_eq!(read_page.get_slice(0, 4).unwrap(), b"test");
    }
    
    #[test]
    fn test_mem_storage_free() {
        let mut storage = MemStorage::new();
        
        let page_id = storage.allocate_page().unwrap();
        assert_eq!(storage.num_pages(), 1);
        
        storage.free_page(page_id).unwrap();
        assert_eq!(storage.num_pages(), 0);
    }
}
