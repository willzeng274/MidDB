use super::block::{Block, BlockIterator};
use super::footer::{BlockHandle, Footer, FOOTER_SIZE};
use crate::bloom::BloomFilter;
use crate::{Error, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

pub struct SSTableReader {
    file: Arc<File>,
    footer: Footer,
    file_size: u64,
    bloom_filter: Option<BloomFilter>,
}

impl SSTableReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        
        let file_size = file.seek(SeekFrom::End(0))?;
        
        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::Corruption("SSTable file too small".to_string()));
        }
        
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_bytes = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_bytes)?;
        
        let footer = Footer::decode(&footer_bytes)?;
        
        let bloom_filter = {
            file.seek(SeekFrom::Start(footer.bloom_handle.offset))?;
            let mut bloom_data = vec![0u8; footer.bloom_handle.size as usize];
            file.read_exact(&mut bloom_data)?;
            BloomFilter::from_bytes_with_meta(&bloom_data)
        };
        
        Ok(SSTableReader {
            file: Arc::new(file),
            footer,
            file_size,
            bloom_filter,
        })
    }
    
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }
        
        let index_block = self.read_block(&self.footer.index_handle)?;
        let mut index_iter = BlockIterator::new(index_block);
        
        index_iter.seek(key);
        
        if !index_iter.valid() {
            return Ok(None);
        }
        
        let handle = BlockHandle::decode(index_iter.value())?;
        
        let data_block = self.read_block(&handle)?;
        let mut data_iter = BlockIterator::new(data_block);
        
        data_iter.seek(key);
        
        if data_iter.valid() && data_iter.key() == key {
            Ok(Some(data_iter.value().to_vec()))
        } else {
            Ok(None)
        }
    }
    
    pub fn iter(&self) -> Result<SSTableIterator> {
        SSTableIterator::new(self)
    }
    
    fn read_block(&self, handle: &BlockHandle) -> Result<Block> {
        let mut file = self.file.as_ref();
        
        file.seek(SeekFrom::Start(handle.offset))?;
        
        let mut data = vec![0u8; handle.size as usize];
        file.read_exact(&mut data)?;
        
        Block::decode(&data)
    }
    
    pub fn footer(&self) -> &Footer {
        &self.footer
    }
}

pub struct SSTableIterator {
    reader: Arc<SSTableReader>,
    index_iter: BlockIterator,
    data_iter: Option<BlockIterator>,
    valid: bool,
}

impl SSTableIterator {
    fn new(reader: &SSTableReader) -> Result<Self> {
        let index_block = reader.read_block(&reader.footer.index_handle)?;
        let mut index_iter = BlockIterator::new(index_block);
        
        index_iter.seek(&[]);
        
        let valid = index_iter.valid();
        let data_iter = if valid {
            let handle = BlockHandle::decode(index_iter.value())?;
            let data_block = reader.read_block(&handle)?;
            let mut iter = BlockIterator::new(data_block);
            iter.seek(&[]);
            Some(iter)
        } else {
            None
        };
        
        Ok(SSTableIterator {
            reader: Arc::new(reader.clone()),
            index_iter,
            data_iter,
            valid,
        })
    }
    
    pub fn key(&self) -> Option<&[u8]> {
        self.data_iter.as_ref().and_then(|iter| {
            if iter.valid() {
                Some(iter.key())
            } else {
                None
            }
        })
    }
    
    pub fn value(&self) -> Option<&[u8]> {
        self.data_iter.as_ref().and_then(|iter| {
            if iter.valid() {
                Some(iter.value())
            } else {
                None
            }
        })
    }
    
    pub fn valid(&self) -> bool {
        self.valid
    }
    
    pub fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.data_iter {
            iter.next();
            
            if !iter.valid() {
                self.index_iter.next();
                
                if self.index_iter.valid() {
                    let handle = BlockHandle::decode(self.index_iter.value())?;
                    let data_block = self.reader.read_block(&handle)?;
                    let mut new_iter = BlockIterator::new(data_block);
                    new_iter.seek(&[]);
                    self.data_iter = Some(new_iter);
                } else {
                    self.valid = false;
                }
            }
        } else {
            self.valid = false;
        }
        
        Ok(())
    }
    
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.index_iter.seek(target);
        
        if !self.index_iter.valid() {
            self.valid = false;
            return Ok(());
        }
        
        let handle = BlockHandle::decode(self.index_iter.value())?;
        let data_block = self.reader.read_block(&handle)?;
        let mut data_iter = BlockIterator::new(data_block);
        data_iter.seek(target);
        
        self.data_iter = Some(data_iter);
        self.valid = self.data_iter.as_ref().map_or(false, |i| i.valid());
        
        Ok(())
    }
}

impl Clone for SSTableReader {
    fn clone(&self) -> Self {
        SSTableReader {
            file: Arc::clone(&self.file),
            footer: self.footer.clone(),
            file_size: self.file_size,
            bloom_filter: self.bloom_filter.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::writer::SSTableWriter;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_sstable_reader_get() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        // Write SSTable
        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        writer.add(b"key3", b"value3").unwrap();
        writer.finish(1, 0).unwrap();
        
        // Read SSTable
        let reader = SSTableReader::open(path).unwrap();
        
        assert_eq!(reader.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(reader.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(reader.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(reader.get(b"key4").unwrap(), None);
    }
    
    #[test]
    fn test_sstable_iterator() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        // Write SSTable
        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish(1, 0).unwrap();
        
        // Iterate through SSTable
        let reader = SSTableReader::open(path).unwrap();
        let mut iter = reader.iter().unwrap();
        
        let mut count = 0;
        while iter.valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            
            let expected_key = format!("key{:03}", count);
            let expected_value = format!("value{}", count);
            
            assert_eq!(key, expected_key.as_bytes());
            assert_eq!(value, expected_value.as_bytes());
            
            iter.next().unwrap();
            count += 1;
        }
        
        assert_eq!(count, 10);
    }
    
    #[test]
    fn test_sstable_seek() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        // Write SSTable
        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        for i in 0..20 {
            let key = format!("key{:03}", i * 2); // Even numbers only
            let value = format!("value{}", i * 2);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish(1, 0).unwrap();
        
        // Test seek
        let reader = SSTableReader::open(path).unwrap();
        let mut iter = reader.iter().unwrap();
        
        // Seek to exact key
        iter.seek(b"key010").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key010");
        
        // Seek to key that doesn't exist (should find next)
        iter.seek(b"key011").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key012");
    }
}
