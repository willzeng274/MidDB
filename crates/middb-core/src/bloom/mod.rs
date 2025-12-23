use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_hash_funcs: u32,
    num_bits: usize,
}

impl BloomFilter {
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let mut num_bits = num_keys * bits_per_key;
        
        if num_bits < 64 {
            num_bits = 64;
        }
        
        let num_hash_funcs = ((bits_per_key as f64) * 0.69) as u32;
        let num_hash_funcs = num_hash_funcs.clamp(1, 30);
        
        let num_bytes = (num_bits + 7) / 8;
        
        assert!(num_bytes <= 1024 * 1024, "Bloom filter too large");
        
        BloomFilter {
            bits: vec![0u8; num_bytes],
            num_hash_funcs,
            num_bits,
        }
    }
    
    pub fn from_bytes(data: &[u8], num_hash_funcs: u32) -> Self {
        BloomFilter {
            bits: data.to_vec(),
            num_hash_funcs,
            num_bits: data.len() * 8,
        }
    }
    
    pub fn insert(&mut self, key: &[u8]) {
        let h = hash(key);
        let delta = (h >> 17) | (h << 15);
        
        for i in 0..self.num_hash_funcs {
            let bit_pos = (h.wrapping_add((i as u64).wrapping_mul(delta))) % (self.num_bits as u64);
            self.set_bit(bit_pos as usize);
        }
    }
    
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let h = hash(key);
        let delta = (h >> 17) | (h << 15);
        
        for i in 0..self.num_hash_funcs {
            let bit_pos = (h.wrapping_add((i as u64).wrapping_mul(delta))) % (self.num_bits as u64);
            if !self.get_bit(bit_pos as usize) {
                return false;
            }
        }
        
        true
    }
    
    pub fn as_bytes(&self) -> &[u8] {
        &self.bits
    }
    
    pub fn num_hash_funcs(&self) -> u32 {
        self.num_hash_funcs
    }
    
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12 + self.bits.len());
        bytes.extend_from_slice(&self.num_hash_funcs.to_le_bytes());
        bytes.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        bytes.extend_from_slice(&self.bits);
        bytes
    }
    
    pub fn from_bytes_with_meta(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }
        
        let num_hash_funcs = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let num_bits = u64::from_le_bytes([
            data[4], data[5], data[6], data[7],
            data[8], data[9], data[10], data[11],
        ]) as usize;
        let bits = data[12..].to_vec();
        
        Some(BloomFilter {
            bits,
            num_hash_funcs,
            num_bits,
        })
    }
    
    fn set_bit(&mut self, pos: usize) {
        let byte_index = pos / 8;
        let bit_index = pos % 8;
        self.bits[byte_index] |= 1 << bit_index;
    }
    
    fn get_bit(&self, pos: usize) -> bool {
        let byte_index = pos / 8;
        let bit_index = pos % 8;
        (self.bits[byte_index] & (1 << bit_index)) != 0
    }
}

fn hash(data: &[u8]) -> u64 {
    let mut hasher = FnvHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

struct FnvHasher {
    state: u64,
}

impl FnvHasher {
    fn new() -> Self {
        FnvHasher {
            state: 0xcbf29ce484222325,
        }
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.state
    }
    
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.state ^= byte as u64;
            self.state = self.state.wrapping_mul(0x100000001b3);
        }
    }
}

pub struct BloomFilterBuilder {
    keys: Vec<Vec<u8>>,
    bits_per_key: usize,
}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: usize) -> Self {
        BloomFilterBuilder {
            keys: Vec::new(),
            bits_per_key,
        }
    }
    
    pub fn add_key(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }
    
    pub fn build(self) -> BloomFilter {
        let mut filter = BloomFilter::new(self.keys.len(), self.bits_per_key);
        
        for key in &self.keys {
            filter.insert(key);
        }
        
        filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::new(100, 10);
        
        // Insert some keys
        filter.insert(b"apple");
        filter.insert(b"banana");
        filter.insert(b"cherry");
        
        // Check membership
        assert!(filter.may_contain(b"apple"));
        assert!(filter.may_contain(b"banana"));
        assert!(filter.may_contain(b"cherry"));
        assert!(!filter.may_contain(b"durian")); // Probably not in the filter
    }
    
    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let num_keys = 1000;
        let bits_per_key = 10;
        
        let mut filter = BloomFilter::new(num_keys, bits_per_key);
        
        // Insert keys
        for i in 0..num_keys {
            let key = format!("key{:06}", i);
            filter.insert(key.as_bytes());
        }
        
        // Check inserted keys (should all be present)
        for i in 0..num_keys {
            let key = format!("key{:06}", i);
            assert!(filter.may_contain(key.as_bytes()));
        }
        
        // Check non-inserted keys (count false positives)
        let num_checks = 10000;
        let mut false_positives = 0;
        
        for i in num_keys..num_keys + num_checks {
            let key = format!("key{:06}", i);
            if filter.may_contain(key.as_bytes()) {
                false_positives += 1;
            }
        }
        
        let fp_rate = false_positives as f64 / num_checks as f64;
        
        // With 10 bits per key, we expect ~1% false positive rate
        // Allow some variance: check that it's less than 2%
        assert!(fp_rate < 0.02, "False positive rate too high: {}", fp_rate);
    }
    
    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::new(100, 10);
        filter.insert(b"test1");
        filter.insert(b"test2");
        
        // Serialize
        let bytes = filter.to_bytes();
        
        // Deserialize
        let restored = BloomFilter::from_bytes_with_meta(&bytes).unwrap();
        
        // Check that membership works
        assert!(restored.may_contain(b"test1"));
        assert!(restored.may_contain(b"test2"));
        assert_eq!(restored.num_hash_funcs(), filter.num_hash_funcs());
    }
    
    #[test]
    fn test_bloom_filter_builder() {
        let mut builder = BloomFilterBuilder::new(10);
        
        builder.add_key(b"key1");
        builder.add_key(b"key2");
        builder.add_key(b"key3");
        
        let filter = builder.build();
        
        assert!(filter.may_contain(b"key1"));
        assert!(filter.may_contain(b"key2"));
        assert!(filter.may_contain(b"key3"));
    }
}

