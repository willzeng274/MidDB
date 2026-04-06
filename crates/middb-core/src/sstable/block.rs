use crate::{Error, Result};

#[derive(Clone)]
pub struct Block {
    data: Vec<u8>,
    restarts: Vec<u32>,
}

impl Default for Block {
    fn default() -> Self {
        Self::new()
    }
}

impl Block {
    pub fn new() -> Self {
        Block {
            data: Vec::new(),
            restarts: vec![0],
        }
    }
    
    pub fn data(&self) -> &[u8] {
        &self.data
    }
    
    pub fn restarts(&self) -> &[u32] {
        &self.restarts
    }
    
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = self.data.clone();
        
        for &restart in &self.restarts {
            encoded.extend_from_slice(&restart.to_le_bytes());
        }
        
        encoded.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        
        encoded
    }
    
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(Error::Corruption("Block too short".to_string()));
        }
        
        let num_restarts_offset = data.len() - 4;
        let num_restarts = u32::from_le_bytes([
            data[num_restarts_offset],
            data[num_restarts_offset + 1],
            data[num_restarts_offset + 2],
            data[num_restarts_offset + 3],
        ]) as usize;
        
        if num_restarts == 0 {
            return Err(Error::Corruption("Block has no restart points".to_string()));
        }
        
        let restarts_offset = num_restarts_offset - num_restarts * 4;
        if restarts_offset > data.len() {
            return Err(Error::Corruption("Invalid restart points".to_string()));
        }
        
        let mut restarts = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let offset = restarts_offset + i * 4;
            let restart = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            restarts.push(restart);
        }
        
        let block_data = data[..restarts_offset].to_vec();
        
        Ok(Block {
            data: block_data,
            restarts,
        })
    }
}

pub struct BlockBuilder {
    block: Block,
    counter: usize,
    restart_interval: usize,
    last_key: Vec<u8>,
    estimated_size: usize,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        BlockBuilder {
            block: Block::new(),
            counter: 0,
            restart_interval,
            last_key: Vec::new(),
            estimated_size: 0,
        }
    }
    
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!key.is_empty(), "Key cannot be empty");
        assert!(
            self.last_key.is_empty() || key > self.last_key.as_slice(),
            "Keys must be added in sorted order"
        );
        
        let mut shared = 0;
        
        // Use prefix compression unless this is a restart point
        if self.counter < self.restart_interval {
            shared = common_prefix_len(&self.last_key, key);
        } else {
            self.block.restarts.push(self.block.data.len() as u32);
            self.counter = 0;
        }
        
        let non_shared = key.len() - shared;
        
        self.append_varint(shared as u64);
        self.append_varint(non_shared as u64);
        self.append_varint(value.len() as u64);
        self.block.data.extend_from_slice(&key[shared..]);
        self.block.data.extend_from_slice(value);
        
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        
        self.counter += 1;
        self.estimated_size = self.estimate_size();
    }
    
    pub fn is_empty(&self) -> bool {
        self.block.data.is_empty()
    }
    
    pub fn current_size_estimate(&self) -> usize {
        self.estimated_size
    }
    
    pub fn finish(self) -> Block {
        self.block
    }
    
    fn append_varint(&mut self, mut value: u64) {
        while value >= 128 {
            self.block.data.push((value & 0x7f) as u8 | 0x80);
            value >>= 7;
        }
        self.block.data.push(value as u8);
    }
    
    fn estimate_size(&self) -> usize {
        self.block.data.len() + self.block.restarts.len() * 4 + 4
    }
}

pub struct BlockIterator {
    data: Vec<u8>,
    restarts: Vec<u32>,
    current: usize,
    restart_index: usize,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl BlockIterator {
    pub fn new(block: Block) -> Self {
        BlockIterator {
            data: block.data,
            restarts: block.restarts,
            current: 0,
            restart_index: 0,
            key: Vec::new(),
            value: Vec::new(),
        }
    }
    
    pub fn seek(&mut self, target: &[u8]) {
        if target.is_empty() || self.restarts.is_empty() {
            self.seek_to_restart_point(0);
        } else {
            // Binary search on restart points to find the last restart whose
            // key is <= target, then linear scan from there.
            let mut lo = 0;
            let mut hi = self.restarts.len() - 1;

            while lo < hi {
                let mid = lo + (hi - lo + 1) / 2;
                let key = self.decode_key_at_restart(mid);
                if key.as_slice() <= target {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
            self.seek_to_restart_point(lo);
        }

        while let Some((key, value)) = self.parse_next_entry() {
            if key.as_slice() >= target {
                self.key = key;
                self.value = value;
                return;
            }
            self.key = key;
            self.value = value;
        }

        self.key.clear();
        self.value.clear();
    }

    /// Decode the full key at the given restart point (restart keys have shared=0).
    /// Returns empty vec on malformed data rather than panicking.
    fn decode_key_at_restart(&self, index: usize) -> Vec<u8> {
        if index >= self.restarts.len() {
            return Vec::new();
        }
        let mut pos = self.restarts[index] as usize;
        if pos >= self.data.len() {
            return Vec::new();
        }

        // Decode shared prefix length (must be 0 at restart points)
        let (shared, bytes) = Self::decode_varint_at(&self.data, pos);
        pos += bytes;
        if shared != 0 {
            return Vec::new(); // malformed restart point
        }
        // Decode non-shared length
        let (non_shared, bytes) = Self::decode_varint_at(&self.data, pos);
        pos += bytes;
        // Skip value length
        let (_, bytes) = Self::decode_varint_at(&self.data, pos);
        pos += bytes;

        if pos + non_shared > self.data.len() {
            return Vec::new(); // truncated block
        }
        self.data[pos..pos + non_shared].to_vec()
    }

    fn decode_varint_at(data: &[u8], mut pos: usize) -> (usize, usize) {
        let start = pos;
        let mut result = 0u64;
        let mut shift = 0;
        loop {
            if pos >= data.len() { return (0, pos - start); }
            let byte = data[pos];
            pos += 1;
            result |= ((byte & 0x7f) as u64) << shift;
            if byte < 128 { return (result as usize, pos - start); }
            shift += 7;
            if shift >= 64 { return (0, pos - start); }
        }
    }
    
    pub fn key(&self) -> &[u8] {
        &self.key
    }
    
    pub fn value(&self) -> &[u8] {
        &self.value
    }
    
    pub fn valid(&self) -> bool {
        !self.key.is_empty()
    }
    
    pub fn next(&mut self) {
        if let Some((key, value)) = self.parse_next_entry() {
            self.key = key;
            self.value = value;
        } else {
            self.key.clear();
            self.value.clear();
        }
    }
    
    fn seek_to_restart_point(&mut self, index: usize) {
        self.key.clear();
        self.restart_index = index;
        self.current = self.restarts[index] as usize;
    }
    
    fn parse_next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.current >= self.data.len() {
            return None;
        }
        
        let shared = self.decode_varint()?;
        let non_shared = self.decode_varint()?;
        let value_len = self.decode_varint()?;
        
        if self.current + non_shared + value_len > self.data.len() {
            return None;
        }
        
        let mut key = Vec::with_capacity(shared + non_shared);
        key.extend_from_slice(&self.key[..shared]);
        key.extend_from_slice(&self.data[self.current..self.current + non_shared]);
        self.current += non_shared;
        
        let value = self.data[self.current..self.current + value_len].to_vec();
        self.current += value_len;
        
        Some((key, value))
    }
    
    fn decode_varint(&mut self) -> Option<usize> {
        let mut result = 0u64;
        let mut shift = 0;
        
        loop {
            if self.current >= self.data.len() {
                return None;
            }
            
            let byte = self.data[self.current];
            self.current += 1;
            
            result |= ((byte & 0x7f) as u64) << shift;
            
            if byte < 128 {
                return Some(result as usize);
            }
            
            shift += 7;
            if shift >= 64 {
                return None;
            }
        }
    }
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let min_len = a.len().min(b.len());
    let mut i = 0;
    while i < min_len && a[i] == b[i] {
        i += 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_block_builder_and_iterator() {
        let mut builder = BlockBuilder::new(2);
        
        builder.add(b"apple", b"red");
        builder.add(b"banana", b"yellow");
        builder.add(b"cherry", b"red");
        builder.add(b"date", b"brown");
        
        let block = builder.finish();
        let mut iter = BlockIterator::new(block);
        
        iter.seek(b"");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"apple");
        assert_eq!(iter.value(), b"red");
        
        iter.next();
        assert_eq!(iter.key(), b"banana");
        assert_eq!(iter.value(), b"yellow");
        
        iter.next();
        assert_eq!(iter.key(), b"cherry");
        assert_eq!(iter.value(), b"red");
        
        iter.next();
        assert_eq!(iter.key(), b"date");
        assert_eq!(iter.value(), b"brown");
        
        iter.next();
        assert!(!iter.valid());
    }
    
    #[test]
    fn test_block_seek() {
        let mut builder = BlockBuilder::new(16);
        
        for i in 0..10 {
            let key = format!("key{i:03}");
            let value = format!("value{i}");
            builder.add(key.as_bytes(), value.as_bytes());
        }
        
        let block = builder.finish();
        let mut iter = BlockIterator::new(block);
        
        iter.seek(b"key005");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"key005");
        assert_eq!(iter.value(), b"value5");
    }
    
    #[test]
    fn test_block_encode_decode() {
        let mut builder = BlockBuilder::new(4);
        
        builder.add(b"foo", b"bar");
        builder.add(b"hello", b"world");
        
        let block = builder.finish();
        let encoded = block.encode();
        let decoded = Block::decode(&encoded).unwrap();
        
        assert_eq!(decoded.data, block.data);
        assert_eq!(decoded.restarts, block.restarts);
    }
}
