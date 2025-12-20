use crate::skiplist::SkipList;
use crate::{Result, sstable::SSTableWriter};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

const NODE_OVERHEAD: usize = 40;

#[derive(Debug, Clone, PartialEq)]
pub enum ValueEntry<V> {
    Value(V),
    Tombstone,
}

impl<V: Default> Default for ValueEntry<V> {
    fn default() -> Self {
        ValueEntry::Value(V::default())
    }
}

pub struct MemTable<K, V> {
    data: SkipList<K, ValueEntry<V>>,
    approx_size: AtomicUsize,
    flush_threshold: usize,
}

impl<K: Ord + Default, V: Default> MemTable<K, V> {
    pub fn new() -> Self {
        Self::with_threshold(64 * 1024 * 1024)
    }

    pub fn with_threshold(flush_threshold: usize) -> Self {
        MemTable {
            data: SkipList::new(),
            approx_size: AtomicUsize::new(0),
            flush_threshold,
        }
    }

    pub fn approx_size(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }

    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold
    }

    pub fn should_flush(&self) -> bool {
        self.approx_size() >= self.flush_threshold
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn put(&mut self, key: K, value: V) -> std::result::Result<(), String>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_size = key.as_ref().len();
        let value_size = value.as_ref().len();
        let entry_size = key_size + value_size + NODE_OVERHEAD;

        self.data.insert(key, ValueEntry::Value(value));
        self.approx_size.fetch_add(entry_size, Ordering::Relaxed);

        Ok(())
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self.data.get(key) {
            Some(ValueEntry::Value(v)) => Some(v),
            Some(ValueEntry::Tombstone) => None,
            None => None,
        }
    }

    pub fn delete(&mut self, key: K) -> std::result::Result<(), String>
    where
        K: AsRef<[u8]>,
    {
        let key_size = key.as_ref().len();
        let entry_size = key_size + NODE_OVERHEAD;

        self.data.insert(key, ValueEntry::Tombstone);
        self.approx_size.fetch_add(entry_size, Ordering::Relaxed);

        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &ValueEntry<V>)> {
        self.data.iter()
    }

    pub fn range<'a>(&'a self, start: &K, end: &'a K) -> impl Iterator<Item = (&'a K, &'a ValueEntry<V>)> {
        self.data.range(start, end)
    }

    pub fn clear(&mut self) {
        self.data = SkipList::new();
        self.approx_size.store(0, Ordering::Relaxed);
    }
    
    pub fn flush_to_sstable<P: AsRef<Path>>(
        &self,
        path: P,
        file_id: u64,
        level: u32,
        block_size: usize,
    ) -> Result<crate::sstable::SSTableMetadata>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut writer = SSTableWriter::create(path, block_size)?;
        
        for (key, entry) in self.iter() {
            let key_bytes: &[u8] = (*key).as_ref();
            match entry {
                ValueEntry::Value(value) => {
                    let value_bytes: &[u8] = (*value).as_ref();
                    writer.add(key_bytes, value_bytes)?;
                }
                ValueEntry::Tombstone => {
                    let tombstone_marker = b"\x00TOMBSTONE";
                    writer.add(key_bytes, tombstone_marker)?;
                }
            }
        }
        
        writer.finish(file_id, level)
    }
}

impl<K: Ord + Default, V: Default> Default for MemTable<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut mt = MemTable::new();
        mt.put("key1".to_string(), "value1".to_string()).unwrap();
        mt.put("key2".to_string(), "value2".to_string()).unwrap();

        assert_eq!(mt.get(&"key1".to_string()), Some(&"value1".to_string()));
        assert_eq!(mt.get(&"key2".to_string()), Some(&"value2".to_string()));
        assert_eq!(mt.get(&"key3".to_string()), None);
    }

    #[test]
    fn test_delete_tombstone() {
        let mut mt = MemTable::new();
        mt.put("key1".to_string(), "value1".to_string()).unwrap();
        assert_eq!(mt.get(&"key1".to_string()), Some(&"value1".to_string()));

        mt.delete("key1".to_string()).unwrap();
        assert_eq!(mt.get(&"key1".to_string()), None);

        // Verify tombstone exists in iterator
        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0].1, ValueEntry::Tombstone));
    }

    #[test]
    fn test_memory_tracking() {
        let mut mt = MemTable::new();
        assert_eq!(mt.approx_size(), 0);

        mt.put("key1".to_string(), "value1".to_string()).unwrap();
        let size_after_first = mt.approx_size();
        assert!(size_after_first > 0);

        mt.put("key2".to_string(), "value2".to_string()).unwrap();
        let size_after_second = mt.approx_size();
        assert!(size_after_second > size_after_first);
    }

    #[test]
    fn test_should_flush() {
        let mut mt = MemTable::with_threshold(100);
        assert!(!mt.should_flush());

        // Add enough data to exceed threshold
        for i in 0..10 {
            mt.put(format!("key{}", i), format!("value{}", i)).unwrap();
        }

        assert!(mt.should_flush());
    }

    #[test]
    fn test_iterator_sorted() {
        let mut mt = MemTable::new();
        mt.put("c".to_string(), "3".to_string()).unwrap();
        mt.put("a".to_string(), "1".to_string()).unwrap();
        mt.put("b".to_string(), "2".to_string()).unwrap();

        let keys: Vec<_> = mt.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_range_query() {
        let mut mt = MemTable::new();
        for i in 0..10 {
            mt.put(format!("key{}", i), format!("value{}", i * 10)).unwrap();
        }

        let items: Vec<_> = mt.range(&"key3".to_string(), &"key7".to_string()).map(|(k, v)| {
            match v {
                ValueEntry::Value(val) => (k.clone(), val.clone()),
                ValueEntry::Tombstone => panic!("Unexpected tombstone"),
            }
        }).collect();

        assert_eq!(items, vec![
            ("key3".to_string(), "value30".to_string()),
            ("key4".to_string(), "value40".to_string()),
            ("key5".to_string(), "value50".to_string()),
            ("key6".to_string(), "value60".to_string())
        ]);
    }

    #[test]
    fn test_clear() {
        let mut mt = MemTable::new();
        mt.put("key1".to_string(), "value1".to_string()).unwrap();
        mt.put("key2".to_string(), "value2".to_string()).unwrap();

        assert!(!mt.is_empty());
        assert!(mt.approx_size() > 0);

        mt.clear();

        assert!(mt.is_empty());
        assert_eq!(mt.approx_size(), 0);
        assert_eq!(mt.get(&"key1".to_string()), None);
    }
}
