use crate::sstable::Block;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

type CacheKey = (u64, u64); // (file_id, block_offset)

const NUM_SHARDS: usize = 16;

struct LruEntry {
    block: Arc<Block>,
    size: usize,
    prev: Option<CacheKey>,
    next: Option<CacheKey>,
}

struct LruShard {
    map: HashMap<CacheKey, LruEntry>,
    head: Option<CacheKey>, // most recently used
    tail: Option<CacheKey>, // least recently used
    current_size: usize,
    capacity: usize,
}

pub struct BlockCache {
    shards: Vec<Mutex<LruShard>>,
}

impl BlockCache {
    pub fn new(capacity: usize) -> Self {
        let per_shard = capacity / NUM_SHARDS;
        let shards = (0..NUM_SHARDS)
            .map(|_| {
                Mutex::new(LruShard {
                    map: HashMap::new(),
                    head: None,
                    tail: None,
                    current_size: 0,
                    capacity: per_shard,
                })
            })
            .collect();
        BlockCache { shards }
    }

    fn shard_index(file_id: u64, offset: u64) -> usize {
        // Simple hash to distribute across shards
        let h = file_id.wrapping_mul(0x9E3779B97F4A7C15) ^ offset.wrapping_mul(0x517CC1B727220A95);
        (h as usize) % NUM_SHARDS
    }

    pub fn get(&self, file_id: u64, offset: u64) -> Option<Arc<Block>> {
        let idx = Self::shard_index(file_id, offset);
        let mut shard = self.shards[idx].lock();
        let key = (file_id, offset);
        if shard.map.contains_key(&key) {
            shard.move_to_front(key);
            Some(Arc::clone(&shard.map[&key].block))
        } else {
            None
        }
    }

    pub fn insert(&self, file_id: u64, offset: u64, block: Block) -> Arc<Block> {
        let idx = Self::shard_index(file_id, offset);
        let mut shard = self.shards[idx].lock();
        let key = (file_id, offset);
        let size = block.data().len() + block.restarts().len() * 4;
        let block = Arc::new(block);

        if shard.map.contains_key(&key) {
            shard.move_to_front(key);
            return Arc::clone(&shard.map[&key].block);
        }

        // Evict until we have room
        while shard.current_size + size > shard.capacity && shard.tail.is_some() {
            shard.evict_lru();
        }

        let entry = LruEntry {
            block: Arc::clone(&block),
            size,
            prev: None,
            next: shard.head,
        };

        if let Some(old_head) = shard.head {
            if let Some(e) = shard.map.get_mut(&old_head) {
                e.prev = Some(key);
            }
        }
        shard.head = Some(key);
        if shard.tail.is_none() {
            shard.tail = Some(key);
        }

        shard.current_size += size;
        shard.map.insert(key, entry);

        block
    }

    pub fn invalidate(&self, file_id: u64) {
        // Must check all shards since blocks from one file can be in any shard
        for shard_mutex in &self.shards {
            let mut shard = shard_mutex.lock();
            let keys: Vec<CacheKey> = shard
                .map
                .keys()
                .filter(|(fid, _)| *fid == file_id)
                .copied()
                .collect();
            for key in keys {
                shard.remove(key);
            }
        }
    }
}

impl LruShard {
    fn move_to_front(&mut self, key: CacheKey) {
        if self.head == Some(key) {
            return;
        }
        self.detach(key);
        if let Some(e) = self.map.get_mut(&key) {
            e.prev = None;
            e.next = self.head;
        }
        if let Some(old_head) = self.head {
            if let Some(e) = self.map.get_mut(&old_head) {
                e.prev = Some(key);
            }
        }
        self.head = Some(key);
        if self.tail.is_none() {
            self.tail = Some(key);
        }
    }

    fn detach(&mut self, key: CacheKey) {
        let (prev, next) = {
            let e = &self.map[&key];
            (e.prev, e.next)
        };
        if let Some(prev_key) = prev {
            if let Some(e) = self.map.get_mut(&prev_key) {
                e.next = next;
            }
        } else {
            self.head = next;
        }
        if let Some(next_key) = next {
            if let Some(e) = self.map.get_mut(&next_key) {
                e.prev = prev;
            }
        } else {
            self.tail = prev;
        }
    }

    fn evict_lru(&mut self) {
        if let Some(tail_key) = self.tail {
            self.remove(tail_key);
        }
    }

    fn remove(&mut self, key: CacheKey) {
        self.detach(key);
        if let Some(entry) = self.map.remove(&key) {
            self.current_size -= entry.size;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic() {
        let cache = BlockCache::new(1024 * 1024);
        let block = Block::new();
        cache.insert(1, 0, block);
        assert!(cache.get(1, 0).is_some());
        assert!(cache.get(1, 100).is_none());
        assert!(cache.get(2, 0).is_none());
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = BlockCache::new(1024 * 1024);
        cache.insert(1, 0, Block::new());
        cache.insert(1, 100, Block::new());
        cache.insert(2, 0, Block::new());

        cache.invalidate(1);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 100).is_none());
        assert!(cache.get(2, 0).is_some());
    }

    #[test]
    fn test_cache_sharding() {
        let cache = BlockCache::new(1024 * 1024);
        // Insert blocks that should go to different shards
        for i in 0..32u64 {
            cache.insert(i, 0, Block::new());
        }
        for i in 0..32u64 {
            assert!(cache.get(i, 0).is_some(), "missing block for file_id={}", i);
        }
    }
}
