use super::page::{Page, PAGE_SIZE};
use crate::{PageId, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub struct FileStorage {
    file: Arc<Mutex<File>>,
    path: PathBuf,
    num_pages: u64,
}

impl FileStorage {
    pub fn create_or_open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let num_pages = file_size / PAGE_SIZE as u64;
        
        Ok(FileStorage {
            file: Arc::new(Mutex::new(file)),
            path,
            num_pages,
        })
    }
    
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        if page_id >= self.num_pages {
            return Err(crate::Error::InvalidArgument(format!(
                "Page ID {} out of bounds (max: {})",
                page_id,
                self.num_pages
            )));
        }
        
        let offset = page_id * PAGE_SIZE as u64;
        let mut file = self.file.lock().unwrap();
        
        file.seek(SeekFrom::Start(offset))?;
        
        let mut data = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut data)?;
        
        Page::from_bytes(data)
    }
    
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        let offset = page_id * PAGE_SIZE as u64;
        let mut file = self.file.lock().unwrap();
        
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(page.data())?;
        
        if page_id >= self.num_pages {
            self.num_pages = page_id + 1;
        }
        
        Ok(())
    }
    
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.num_pages;
        let page = Page::new();
        self.write_page(page_id, &page)?;
        Ok(page_id)
    }
    
    pub fn sync(&self) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_all()?;
        Ok(())
    }
    
    pub fn num_pages(&self) -> u64 {
        self.num_pages
    }
    
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_file_storage() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut storage = FileStorage::create_or_open(path).unwrap();
        
        // Allocate and write a page
        let page_id = storage.allocate_page().unwrap();
        assert_eq!(page_id, 0);
        
        let mut page = Page::new();
        page.write_at(0, b"test data").unwrap();
        storage.write_page(page_id, &page).unwrap();
        
        // Read it back
        let read_page = storage.read_page(page_id).unwrap();
        assert_eq!(read_page.get_slice(0, 9).unwrap(), b"test data");
    }
    
    #[test]
    fn test_file_storage_multiple_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut storage = FileStorage::create_or_open(path).unwrap();
        
        // Allocate multiple pages
        let page0 = storage.allocate_page().unwrap();
        let page1 = storage.allocate_page().unwrap();
        let page2 = storage.allocate_page().unwrap();
        
        assert_eq!(page0, 0);
        assert_eq!(page1, 1);
        assert_eq!(page2, 2);
        
        assert_eq!(storage.num_pages(), 3);
    }
}
