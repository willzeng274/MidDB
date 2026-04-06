use super::reader::SSTableIterator;
use crate::Result;

pub struct MergeIterator {
    iters: Vec<SSTableIterator>,
    current_index: Option<usize>,
}

impl MergeIterator {
    pub fn new(iters: Vec<SSTableIterator>) -> Self {
        MergeIterator {
            iters,
            current_index: None,
        }
    }
    
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.current_index = self.find_smallest()?;
        // Skip duplicates of the initial key in other iterators
        self.skip_duplicates()?;
        Ok(())
    }
    
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        for iter in &mut self.iters {
            iter.seek(target)?;
        }

        self.current_index = self.find_smallest()?;
        self.skip_duplicates()?;
        Ok(())
    }
    
    pub fn key(&self) -> Option<&[u8]> {
        self.current_index
            .and_then(|idx| self.iters[idx].key())
    }
    
    pub fn value(&self) -> Option<&[u8]> {
        self.current_index
            .and_then(|idx| self.iters[idx].value())
    }
    
    pub fn valid(&self) -> bool {
        self.current_index.is_some()
    }
    
    pub fn next(&mut self) -> Result<()> {
        if let Some(idx) = self.current_index {
            self.iters[idx].next()?;
        }

        self.current_index = self.find_smallest()?;
        // After finding the next smallest, skip any other iterators sitting on the same key
        self.skip_duplicates()?;
        Ok(())
    }

    /// When multiple iterators point at the same key, advance all but the winning one
    /// (lowest index = newest data). This ensures each key is emitted exactly once,
    /// with the value from the newest SSTable.
    fn skip_duplicates(&mut self) -> Result<()> {
        if let Some(winner) = self.current_index {
            if let Some(winner_key) = self.iters[winner].key().map(|k| k.to_vec()) {
                for (i, iter) in self.iters.iter_mut().enumerate() {
                    if i != winner && iter.valid() {
                        if let Some(k) = iter.key() {
                            if k == winner_key.as_slice() {
                                iter.next()?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn find_smallest(&self) -> Result<Option<usize>> {
        let mut smallest_idx = None;
        let mut smallest_key: Option<Vec<u8>> = None;

        for (idx, iter) in self.iters.iter().enumerate() {
            if iter.valid() {
                if let Some(key) = iter.key() {
                    match &smallest_key {
                        None => {
                            smallest_key = Some(key.to_vec());
                            smallest_idx = Some(idx);
                        }
                        Some(current_smallest) => {
                            if key < current_smallest.as_slice() {
                                smallest_key = Some(key.to_vec());
                                smallest_idx = Some(idx);
                            }
                        }
                    }
                }
            }
        }

        Ok(smallest_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::writer::SSTableWriter;
    use super::super::reader::SSTableReader;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_merge_iterator() {
        // Create two SSTables
        let temp1 = NamedTempFile::new().unwrap();
        let temp2 = NamedTempFile::new().unwrap();
        
        // Write first SSTable with odd keys
        let mut writer1 = SSTableWriter::create(temp1.path(), 4096).unwrap();
        for i in (1..10).step_by(2) {
            let key = format!("key{i:02}");
            let value = format!("value{i}");
            writer1.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer1.finish(1, 0).unwrap();
        
        // Write second SSTable with even keys
        let mut writer2 = SSTableWriter::create(temp2.path(), 4096).unwrap();
        for i in (0..10).step_by(2) {
            let key = format!("key{i:02}");
            let value = format!("value{i}");
            writer2.add(key.as_bytes(), value.as_bytes()).unwrap();
        }
        writer2.finish(2, 0).unwrap();
        
        // Create merge iterator
        let reader1 = SSTableReader::open(temp1.path()).unwrap();
        let reader2 = SSTableReader::open(temp2.path()).unwrap();
        
        let iter1 = reader1.iter().unwrap();
        let iter2 = reader2.iter().unwrap();
        
        let mut merge = MergeIterator::new(vec![iter1, iter2]);
        merge.seek_to_first().unwrap();
        
        // Verify merged order
        for i in 0..10 {
            assert!(merge.valid());
            let expected_key = format!("key{i:02}");
            assert_eq!(merge.key().unwrap(), expected_key.as_bytes());
            merge.next().unwrap();
        }
        
        assert!(!merge.valid());
    }
}
