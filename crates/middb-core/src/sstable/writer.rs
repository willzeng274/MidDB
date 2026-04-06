use super::block::{Block, BlockBuilder};
use super::footer::{BlockHandle, Footer, SSTableMetadata, FOOTER_SIZE};
use crate::bloom::BloomFilterBuilder;
use crate::compression::{self, CompressionType};
use crate::Result;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct SSTableWriter {
    file: BufWriter<File>,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
    bloom_builder: BloomFilterBuilder,
    block_size: usize,
    offset: u64,
    pending_index_entry: Option<(Vec<u8>, BlockHandle)>,
    num_entries: u64,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
    compression: CompressionType,
}

impl SSTableWriter {
    pub fn create<P: AsRef<Path>>(path: P, block_size: usize) -> Result<Self> {
        Self::create_with_options(path, block_size, 10, CompressionType::None)
    }

    pub fn create_with_bloom_bits<P: AsRef<Path>>(
        path: P,
        block_size: usize,
        bloom_bits_per_key: usize,
    ) -> Result<Self> {
        Self::create_with_options(path, block_size, bloom_bits_per_key, CompressionType::None)
    }

    pub fn create_with_options<P: AsRef<Path>>(
        path: P,
        block_size: usize,
        bloom_bits_per_key: usize,
        compression: CompressionType,
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        Ok(SSTableWriter {
            file: BufWriter::new(file),
            data_block_builder: BlockBuilder::new(16),
            index_block_builder: BlockBuilder::new(1),
            bloom_builder: BloomFilterBuilder::new(bloom_bits_per_key),
            block_size,
            offset: 0,
            pending_index_entry: None,
            num_entries: 0,
            smallest_key: None,
            largest_key: None,
            compression,
        })
    }
    
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "Key cannot be empty");
        
        if self.smallest_key.is_none() {
            self.smallest_key = Some(key.to_vec());
        }
        self.largest_key = Some(key.to_vec());
        
        if let Some((last_key, handle)) = self.pending_index_entry.take() {
            let separator = find_shortest_separator(&last_key, key);
            self.add_index_entry(&separator, handle)?;
        }
        
        self.data_block_builder.add(key, value);
        self.num_entries += 1;
        self.bloom_builder.add_key(key);
        
        if self.data_block_builder.current_size_estimate() >= self.block_size {
            self.flush_data_block()?;
        }
        
        Ok(())
    }
    
    pub fn finish(mut self, file_id: u64, level: u32) -> Result<SSTableMetadata> {
        if !self.data_block_builder.is_empty() {
            self.flush_data_block()?;
        }
        
        if let Some((last_key, handle)) = self.pending_index_entry.take() {
            self.add_index_entry(&last_key, handle)?;
        }
        
        let bloom_handle = self.write_bloom_filter_block()?;
        
        let index_block_builder = std::mem::replace(
            &mut self.index_block_builder,
            BlockBuilder::new(1),
        );
        let index_block = index_block_builder.finish();
        let index_handle = self.write_block_raw(&index_block)?;
        
        let footer = Footer::with_compression(index_handle, bloom_handle, self.compression);
        self.file.write_all(&footer.encode())?;
        self.offset += FOOTER_SIZE as u64;
        
        self.file.flush()?;
        self.file.get_mut().sync_all()?;

        Ok(SSTableMetadata::new(
            file_id,
            self.offset,
            self.smallest_key.unwrap_or_default(),
            self.largest_key.unwrap_or_default(),
            self.num_entries,
            level,
        ))
    }
    
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.is_empty() {
            return Ok(());
        }
        
        let block = std::mem::replace(
            &mut self.data_block_builder,
            BlockBuilder::new(16),
        ).finish();
        
        let last_key = self.largest_key.clone().unwrap_or_default();
        let handle = self.write_block(&block)?;
        
        self.pending_index_entry = Some((last_key, handle));
        
        Ok(())
    }
    
    fn write_block(&mut self, block: &Block) -> Result<BlockHandle> {
        self.write_block_with_compression(block, self.compression)
    }

    fn write_block_raw(&mut self, block: &Block) -> Result<BlockHandle> {
        self.write_block_with_compression(block, CompressionType::None)
    }

    fn write_block_with_compression(
        &mut self,
        block: &Block,
        compression: CompressionType,
    ) -> Result<BlockHandle> {
        let encoded = block.encode();
        let data = compression::compress(&encoded, compression);
        let offset = self.offset;
        let size = data.len() as u64;

        self.file.write_all(&data)?;
        self.offset += size;

        Ok(BlockHandle::new(offset, size))
    }
    
    fn add_index_entry(&mut self, key: &[u8], handle: BlockHandle) -> Result<()> {
        let handle_bytes = handle.encode();
        self.index_block_builder.add(key, &handle_bytes);
        Ok(())
    }
    
    fn write_bloom_filter_block(&mut self) -> Result<BlockHandle> {
        let offset = self.offset;
        
        let bloom_builder = std::mem::replace(
            &mut self.bloom_builder,
            BloomFilterBuilder::new(10),
        );
        let bloom_filter = bloom_builder.build();
        let bloom_bytes = bloom_filter.to_bytes();
        
        self.file.write_all(&bloom_bytes)?;
        self.offset += bloom_bytes.len() as u64;
        
        Ok(BlockHandle::new(offset, bloom_bytes.len() as u64))
    }
}

fn find_shortest_separator(last_key: &[u8], current_key: &[u8]) -> Vec<u8> {
    let min_len = last_key.len().min(current_key.len());
    let mut diff_index = 0;
    
    while diff_index < min_len && last_key[diff_index] == current_key[diff_index] {
        diff_index += 1;
    }
    
    if diff_index >= min_len {
        return last_key.to_vec();
    }
    
    if diff_index < last_key.len() && last_key[diff_index] < 0xff
        && last_key[diff_index] + 1 < current_key[diff_index]
    {
        let mut result = last_key[..=diff_index].to_vec();
        result[diff_index] += 1;
        return result;
    }
    
    last_key.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_sstable_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut writer = SSTableWriter::create(path, 4096).unwrap();
        
        writer.add(b"apple", b"red").unwrap();
        writer.add(b"banana", b"yellow").unwrap();
        writer.add(b"cherry", b"red").unwrap();
        
        let metadata = writer.finish(1, 0).unwrap();
        
        assert_eq!(metadata.file_id, 1);
        assert_eq!(metadata.level, 0);
        assert_eq!(metadata.num_entries, 3);
        assert_eq!(metadata.smallest_key, b"apple");
        assert_eq!(metadata.largest_key, b"cherry");
    }
    
    #[test]
    fn test_find_shortest_separator() {
        // Normal case
        let sep = find_shortest_separator(b"abc", b"acd");
        assert!(sep >= b"abc".to_vec() && sep < b"acd".to_vec());
        
        // Prefix case
        let sep = find_shortest_separator(b"abc", b"abcd");
        assert_eq!(sep, b"abc".to_vec());
        
        // Can increment
        let sep = find_shortest_separator(b"ab", b"ad");
        assert_eq!(sep, b"ac".to_vec());
    }
}
