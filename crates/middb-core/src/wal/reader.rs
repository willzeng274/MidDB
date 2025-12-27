use super::entry::WalEntry;
use crate::Result;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub struct WalReader {
    reader: BufReader<File>,
    offset: u64,
}

impl WalReader {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        
        Ok(WalReader {
            reader: BufReader::new(file),
            offset: 0,
        })
    }
    
    pub fn next_entry(&mut self) -> Result<Option<WalEntry>> {
        let mut header = [0u8; 8];
        
        match self.reader.read_exact(&mut header) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        }
        
        let data_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
        
        let mut data = vec![0u8; data_len];
        self.reader.read_exact(&mut data)?;
        
        let mut full_entry = Vec::with_capacity(8 + data_len);
        full_entry.extend_from_slice(&header);
        full_entry.extend_from_slice(&data);
        
        let (entry, size) = WalEntry::decode(&full_entry)?;
        self.offset += size as u64;
        
        Ok(Some(entry))
    }
    
    pub fn read_all(&mut self) -> Result<Vec<WalEntry>> {
        let mut entries = Vec::new();
        
        while let Some(entry) = self.next_entry()? {
            entries.push(entry);
        }
        
        Ok(entries)
    }
    
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::writer::WalWriter;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_wal_reader() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        // Write some entries
        {
            let mut writer = WalWriter::create(path).unwrap();
            
            writer.append(&WalEntry::put(1, b"key1".to_vec(), b"value1".to_vec())).unwrap();
            writer.append(&WalEntry::put(2, b"key2".to_vec(), b"value2".to_vec())).unwrap();
            writer.append(&WalEntry::delete(3, b"key3".to_vec())).unwrap();
            writer.sync().unwrap();
        }
        
        // Read them back
        let mut reader = WalReader::open(path).unwrap();
        let entries = reader.read_all().unwrap();
        
        assert_eq!(entries.len(), 3);
        
        assert_eq!(entries[0].sequence_number, 1);
        assert_eq!(entries[0].key, b"key1");
        
        assert_eq!(entries[1].sequence_number, 2);
        assert_eq!(entries[1].key, b"key2");
        
        assert_eq!(entries[2].sequence_number, 3);
        assert_eq!(entries[2].key, b"key3");
        assert_eq!(entries[2].value, None);
    }
}
