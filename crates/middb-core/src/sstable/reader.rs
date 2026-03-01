use super::block::{Block, BlockIterator};
use super::footer::{BlockHandle, Footer, FOOTER_SIZE};
use crate::bloom::BloomFilter;
use crate::cache::BlockCache;
use crate::compression;
use crate::{Error, Result};
use std::fs::File;
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

pub struct SSTableReader {
    file: Arc<File>,
    footer: Footer,
    file_size: u64,
    file_id: u64,
    bloom_filter: Option<BloomFilter>,
    cache: Option<Arc<BlockCache>>,
    cached_index: Option<Arc<Block>>,
}

impl SSTableReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_cache(path, 0, None)
    }

    pub fn open_with_cache<P: AsRef<Path>>(path: P, file_id: u64, cache: Option<Arc<BlockCache>>) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(Error::Corruption("SSTable file too small".to_string()));
        }

        // Read footer from end of file using pread (no seek needed)
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let mut footer_bytes = [0u8; FOOTER_SIZE];
        #[cfg(unix)]
        {
            file.read_at(&mut footer_bytes, footer_offset)?;
        }
        #[cfg(not(unix))]
        {
            let mut f = &file;
            f.seek(SeekFrom::Start(footer_offset))?;
            f.read_exact(&mut footer_bytes)?;
        }

        let footer = Footer::decode(&footer_bytes)?;

        let bloom_filter = {
            let mut bloom_data = vec![0u8; footer.bloom_handle.size as usize];
            #[cfg(unix)]
            {
                file.read_at(&mut bloom_data, footer.bloom_handle.offset)?;
            }
            #[cfg(not(unix))]
            {
                let mut f = &file;
                f.seek(SeekFrom::Start(footer.bloom_handle.offset))?;
                f.read_exact(&mut bloom_data)?;
            }
            BloomFilter::from_bytes_with_meta(&bloom_data)
        };

        // Pre-cache the index block
        let cached_index = {
            let mut data = vec![0u8; footer.index_handle.size as usize];
            #[cfg(unix)]
            {
                file.read_at(&mut data, footer.index_handle.offset)?;
            }
            #[cfg(not(unix))]
            {
                let mut f = &file;
                f.seek(SeekFrom::Start(footer.index_handle.offset))?;
                f.read_exact(&mut data)?;
            }
            let decompressed = compression::decompress(&data)?;
            Some(Arc::new(Block::decode(&decompressed)?))
        };

        Ok(SSTableReader {
            file: Arc::new(file),
            footer,
            file_size,
            file_id,
            bloom_filter,
            cache,
            cached_index,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(ref bloom) = self.bloom_filter {
            if !bloom.may_contain(key) {
                return Ok(None);
            }
        }

        // Use cached index block
        let index_block = if let Some(ref cached) = self.cached_index {
            (**cached).clone()
        } else {
            self.read_block_raw(&self.footer.index_handle)?
        };
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
        // Check cache first
        if let Some(ref cache) = self.cache {
            if let Some(cached_block) = cache.get(self.file_id, handle.offset) {
                return Ok((*cached_block).clone());
            }
        }

        let block = self.read_block_raw(handle)?;

        // Insert into cache
        if let Some(ref cache) = self.cache {
            cache.insert(self.file_id, handle.offset, block.clone());
        }

        Ok(block)
    }

    fn read_block_raw(&self, handle: &BlockHandle) -> Result<Block> {
        let mut data = vec![0u8; handle.size as usize];
        // Use pread (positioned read) to avoid seek+read race when multiple
        // threads share the same Arc<File>. pread is atomic and doesn't
        // modify the file offset.
        #[cfg(unix)]
        {
            self.file.read_at(&mut data, handle.offset)?;
        }
        #[cfg(not(unix))]
        {
            let mut file = self.file.as_ref();
            file.seek(SeekFrom::Start(handle.offset))?;
            file.read_exact(&mut data)?;
        }
        let decompressed = compression::decompress(&data)?;
        Block::decode(&decompressed)
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
        let index_block = if let Some(ref cached) = reader.cached_index {
            (**cached).clone()
        } else {
            reader.read_block_raw(&reader.footer.index_handle)?
        };
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
            file_id: self.file_id,
            bloom_filter: self.bloom_filter.clone(),
            cache: self.cache.clone(),
            cached_index: self.cached_index.clone(),
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

        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        writer.add(b"key3", b"value3").unwrap();
        writer.finish(1, 0).unwrap();

        let reader = SSTableReader::open(path).unwrap();

        assert_eq!(reader.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(reader.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(reader.get(b"key3").unwrap(), Some(b"value3".to_vec()));
        assert_eq!(reader.get(b"key4").unwrap(), None);
    }

    #[test]
    fn test_sstable_reader_with_cache() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        writer.finish(1, 0).unwrap();

        let cache = Arc::new(BlockCache::new(64 * 1024 * 1024));
        let reader = SSTableReader::open_with_cache(path, 1, Some(cache.clone())).unwrap();

        // First read — cache miss
        assert_eq!(reader.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        // Second read — cache hit
        assert_eq!(reader.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(reader.get(b"key2").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_sstable_iterator() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        for i in 0..10 {
            let key = format!("key{:03}", i);
            let value = format!("value{}", i);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish(1, 0).unwrap();

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

        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        for i in 0..20 {
            let key = format!("key{:03}", i * 2);
            let value = format!("value{}", i * 2);
            writer.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer.finish(1, 0).unwrap();

        let reader = SSTableReader::open(path).unwrap();
        let mut iter = reader.iter().unwrap();

        iter.seek(b"key010").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key010");

        iter.seek(b"key011").unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"key012");
    }
}
