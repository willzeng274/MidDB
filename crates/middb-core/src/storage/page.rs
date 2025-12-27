use crate::Result;

pub const PAGE_SIZE: usize = 4096;

#[derive(Clone)]
pub struct Page {
    data: Vec<u8>,
}

impl Page {
    pub fn new() -> Self {
        Page {
            data: vec![0u8; PAGE_SIZE],
        }
    }
    
    pub fn from_bytes(data: Vec<u8>) -> Result<Self> {
        if data.len() != PAGE_SIZE {
            return Err(crate::Error::InvalidArgument(format!(
                "Page data must be {} bytes, got {}",
                PAGE_SIZE,
                data.len()
            )));
        }
        
        Ok(Page { data })
    }
    
    pub fn data(&self) -> &[u8] {
        &self.data
    }
    
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    pub fn get_slice(&self, offset: usize, len: usize) -> Result<&[u8]> {
        if offset + len > PAGE_SIZE {
            return Err(crate::Error::InvalidArgument(format!(
                "Slice out of bounds: offset={}, len={}, page_size={}",
                offset, len, PAGE_SIZE
            )));
        }
        
        Ok(&self.data[offset..offset + len])
    }
    
    pub fn write_at(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > PAGE_SIZE {
            return Err(crate::Error::InvalidArgument(format!(
                "Write out of bounds: offset={}, len={}, page_size={}",
                offset,
                data.len(),
                PAGE_SIZE
            )));
        }
        
        self.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }
    
    pub fn zero(&mut self) {
        self.data.fill(0);
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_page_creation() {
        let page = Page::new();
        assert_eq!(page.data().len(), PAGE_SIZE);
    }
    
    #[test]
    fn test_page_write_read() {
        let mut page = Page::new();
        
        let test_data = b"Hello, World!";
        page.write_at(100, test_data).unwrap();
        
        let read_data = page.get_slice(100, test_data.len()).unwrap();
        assert_eq!(read_data, test_data);
    }
    
    #[test]
    fn test_page_out_of_bounds() {
        let mut page = Page::new();
        
        let large_data = vec![0u8; PAGE_SIZE + 1];
        assert!(page.write_at(0, &large_data).is_err());
        
        assert!(page.get_slice(PAGE_SIZE - 10, 20).is_err());
    }
}
