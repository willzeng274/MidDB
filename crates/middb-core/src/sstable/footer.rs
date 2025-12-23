use crate::{Error, Result};

const SSTABLE_MAGIC: u64 = 0x5354414254414244;
const FOOTER_VERSION: u32 = 1;

pub const FOOTER_SIZE: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        BlockHandle { offset, size }
    }
    
    pub fn encode(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&self.offset.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.size.to_le_bytes());
        bytes
    }
    
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(Error::Corruption("BlockHandle too short".to_string()));
        }
        
        let offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let size = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        
        Ok(BlockHandle { offset, size })
    }
}

#[derive(Debug, Clone)]
pub struct Footer {
    pub index_handle: BlockHandle,
    pub bloom_handle: BlockHandle,
    pub version: u32,
}

impl Footer {
    pub fn new(index_handle: BlockHandle, bloom_handle: BlockHandle) -> Self {
        Footer {
            index_handle,
            bloom_handle,
            version: FOOTER_VERSION,
        }
    }
    
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut bytes = [0u8; FOOTER_SIZE];
        
        bytes[0..16].copy_from_slice(&self.index_handle.encode());
        bytes[16..32].copy_from_slice(&self.bloom_handle.encode());
        bytes[32..36].copy_from_slice(&self.version.to_le_bytes());
        bytes[40..48].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        
        bytes
    }
    
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != FOOTER_SIZE {
            return Err(Error::Corruption(format!(
                "Invalid footer size: expected {}, got {}",
                FOOTER_SIZE,
                bytes.len()
            )));
        }
        
        let magic = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(Error::Corruption(format!(
                "Invalid SSTable magic number: expected {:#x}, got {:#x}",
                SSTABLE_MAGIC, magic
            )));
        }
        
        let index_handle = BlockHandle::decode(&bytes[0..16])?;
        let bloom_handle = BlockHandle::decode(&bytes[16..32])?;
        
        let version = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
        if version != FOOTER_VERSION {
            return Err(Error::Corruption(format!(
                "Unsupported SSTable version: {}",
                version
            )));
        }
        
        Ok(Footer {
            index_handle,
            bloom_handle,
            version,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SSTableMetadata {
    pub file_id: u64,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub num_entries: u64,
    pub level: u32,
}

impl SSTableMetadata {
    pub fn new(
        file_id: u64,
        file_size: u64,
        smallest_key: Vec<u8>,
        largest_key: Vec<u8>,
        num_entries: u64,
        level: u32,
    ) -> Self {
        SSTableMetadata {
            file_id,
            file_size,
            smallest_key,
            largest_key,
            num_entries,
            level,
        }
    }
    
    pub fn may_contain(&self, key: &[u8]) -> bool {
        key >= self.smallest_key.as_slice() && key <= self.largest_key.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_block_handle_encode_decode() {
        let handle = BlockHandle::new(12345, 67890);
        let encoded = handle.encode();
        let decoded = BlockHandle::decode(&encoded).unwrap();
        
        assert_eq!(handle, decoded);
    }
    
    #[test]
    fn test_footer_encode_decode() {
        let footer = Footer::new(
            BlockHandle::new(100, 200),
            BlockHandle::new(300, 400),
        );
        
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded).unwrap();
        
        assert_eq!(footer.index_handle, decoded.index_handle);
        assert_eq!(footer.bloom_handle, decoded.bloom_handle);
        assert_eq!(footer.version, decoded.version);
    }
    
    #[test]
    fn test_footer_invalid_magic() {
        let bytes = [0u8; FOOTER_SIZE];
        // Don't set the magic number correctly
        
        let result = Footer::decode(&bytes);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_sstable_metadata() {
        let metadata = SSTableMetadata::new(
            1,
            10000,
            b"apple".to_vec(),
            b"zebra".to_vec(),
            100,
            0,
        );
        
        assert!(metadata.may_contain(b"banana"));
        assert!(metadata.may_contain(b"apple"));
        assert!(metadata.may_contain(b"zebra"));
        assert!(!metadata.may_contain(b"aaa"));
        assert!(!metadata.may_contain(b"zzz"));
    }
}
