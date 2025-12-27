use super::entry::WalEntry;
use crate::Result;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    bytes_written: u64,
}

impl WalWriter {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        
        Ok(WalWriter {
            file: BufWriter::new(file),
            path,
            bytes_written: 0,
        })
    }
    
    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let encoded = entry.encode();
        self.file.write_all(&encoded)?;
        self.bytes_written += encoded.len() as u64;
        Ok(())
    }
    
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush()?;
        Ok(())
    }
    
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_all()?;
        Ok(())
    }
    
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
    
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_wal_writer() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();
        
        let mut writer = WalWriter::create(path).unwrap();
        
        let entry1 = WalEntry::put(1, b"key1".to_vec(), b"value1".to_vec());
        let entry2 = WalEntry::put(2, b"key2".to_vec(), b"value2".to_vec());
        
        writer.append(&entry1).unwrap();
        writer.append(&entry2).unwrap();
        writer.sync().unwrap();
        
        assert!(writer.bytes_written() > 0);
    }
}
