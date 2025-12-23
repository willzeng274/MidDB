use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStyle {
    Leveled,
    Universal,
}

impl Default for CompactionStyle {
    fn default() -> Self {
        CompactionStyle::Leveled
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub memtable_size: usize,
    pub wal_dir: PathBuf,
    pub data_dir: PathBuf,
    pub max_open_files: usize,
    pub compaction_style: CompactionStyle,
    pub bloom_bits_per_key: usize,
    pub block_size: usize,
    pub use_compression: bool,
    pub level0_file_num_compaction_trigger: usize,
    pub max_bytes_for_level_base: u64,
    pub max_bytes_for_level_multiplier: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            memtable_size: 64 * 1024 * 1024,
            wal_dir: PathBuf::from("./wal"),
            data_dir: PathBuf::from("./data"),
            max_open_files: 1000,
            compaction_style: CompactionStyle::Leveled,
            bloom_bits_per_key: 10,
            block_size: 64 * 1024,
            use_compression: false,
            level0_file_num_compaction_trigger: 4,
            max_bytes_for_level_base: 10 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10,
        }
    }
}

impl Config {
    pub fn new<P: Into<PathBuf>>(data_dir: P) -> Self {
        let data_dir = data_dir.into();
        let wal_dir = data_dir.join("wal");
        
        Config {
            data_dir,
            wal_dir,
            ..Default::default()
        }
    }
    
    pub fn validate(&self) -> Result<(), String> {
        if self.memtable_size < 1024 * 1024 {
            return Err("memtable_size must be at least 1 MB".to_string());
        }
        
        if self.block_size < 4096 {
            return Err("block_size must be at least 4 KB".to_string());
        }
        
        if self.bloom_bits_per_key == 0 {
            return Err("bloom_bits_per_key must be greater than 0".to_string());
        }
        
        if self.level0_file_num_compaction_trigger < 2 {
            return Err("level0_file_num_compaction_trigger must be at least 2".to_string());
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }
    
    #[test]
    fn test_new_config() {
        let config = Config::new("/tmp/testdb");
        assert_eq!(config.data_dir, PathBuf::from("/tmp/testdb"));
        assert_eq!(config.wal_dir, PathBuf::from("/tmp/testdb/wal"));
    }
    
    #[test]
    fn test_invalid_memtable_size() {
        let mut config = Config::default();
        config.memtable_size = 1024; // Too small
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_invalid_block_size() {
        let mut config = Config::default();
        config.block_size = 1024; // Too small
        assert!(config.validate().is_err());
    }
}
