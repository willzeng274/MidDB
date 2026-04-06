use crate::sstable::SSTableMetadata;
use crate::{Error, Result};
use std::fs;
use std::io::Write;
use std::path::Path;

const MANIFEST_MAGIC: &[u8; 8] = b"MIDDBMAN";
const MANIFEST_VERSION: u32 = 1;

/// Persistent record of all SSTable files and their levels.
/// Written atomically (write to temp, then rename) after every flush and compaction.
#[derive(Debug, Clone)]
pub struct ManifestRecord {
    pub next_file_id: u64,
    pub sequence_number: u64,
    pub files: Vec<ManifestFileEntry>,
}

#[derive(Debug, Clone)]
pub struct ManifestFileEntry {
    pub file_id: u64,
    pub level: u32,
    pub file_size: u64,
    pub num_entries: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

impl ManifestFileEntry {
    pub fn from_metadata(m: &SSTableMetadata) -> Self {
        ManifestFileEntry {
            file_id: m.file_id,
            level: m.level,
            file_size: m.file_size,
            num_entries: m.num_entries,
            smallest_key: m.smallest_key.clone(),
            largest_key: m.largest_key.clone(),
        }
    }

    pub fn to_metadata(&self) -> SSTableMetadata {
        SSTableMetadata::new(
            self.file_id,
            self.file_size,
            self.smallest_key.clone(),
            self.largest_key.clone(),
            self.num_entries,
            self.level,
        )
    }
}

impl ManifestRecord {
    /// Encode the manifest to bytes.
    /// Format: magic(8) | version(4) | next_file_id(8) | sequence_number(8) | num_files(4) | [file entries...] | crc32(4)
    /// Each file entry: file_id(8) | level(4) | file_size(8) | num_entries(8) | key_len(4) | smallest_key | key_len(4) | largest_key
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.extend_from_slice(MANIFEST_MAGIC);
        buf.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        buf.extend_from_slice(&self.next_file_id.to_le_bytes());
        buf.extend_from_slice(&self.sequence_number.to_le_bytes());
        buf.extend_from_slice(&(self.files.len() as u32).to_le_bytes());

        for f in &self.files {
            buf.extend_from_slice(&f.file_id.to_le_bytes());
            buf.extend_from_slice(&f.level.to_le_bytes());
            buf.extend_from_slice(&f.file_size.to_le_bytes());
            buf.extend_from_slice(&f.num_entries.to_le_bytes());
            buf.extend_from_slice(&(f.smallest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&f.smallest_key);
            buf.extend_from_slice(&(f.largest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&f.largest_key);
        }

        let crc = crc32(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 36 {
            return Err(Error::Corruption("Manifest too short".to_string()));
        }

        // Verify CRC (last 4 bytes)
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().unwrap());
        let computed_crc = crc32(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(Error::Corruption("Manifest CRC mismatch".to_string()));
        }

        let mut off = 0;

        // Magic
        if &data[off..off + 8] != MANIFEST_MAGIC {
            return Err(Error::Corruption("Invalid manifest magic".to_string()));
        }
        off += 8;

        // Version
        let version = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
        if version != MANIFEST_VERSION {
            return Err(Error::Corruption(format!("Unsupported manifest version: {version}")));
        }
        off += 4;

        let next_file_id = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;

        let sequence_number = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;

        let num_files = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        off += 4;

        // Sanity check: each file entry is at least 36 bytes (28 fixed + 4+0 + 4+0 keys)
        let max_possible = crc_offset.saturating_sub(off) / 36;
        if num_files > max_possible {
            return Err(Error::Corruption("Manifest num_files exceeds file size".to_string()));
        }

        let mut files = Vec::with_capacity(num_files);
        for _ in 0..num_files {
            if off + 28 > crc_offset {
                return Err(Error::Corruption("Manifest truncated in file entry".to_string()));
            }

            let file_id = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            let level = u32::from_le_bytes(data[off..off + 4].try_into().unwrap());
            off += 4;
            let file_size = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;
            let num_entries = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            off += 8;

            let sk_len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            if off + sk_len > crc_offset {
                return Err(Error::Corruption("Manifest truncated in smallest_key".to_string()));
            }
            let smallest_key = data[off..off + sk_len].to_vec();
            off += sk_len;

            let lk_len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            if off + lk_len > crc_offset {
                return Err(Error::Corruption("Manifest truncated in largest_key".to_string()));
            }
            let largest_key = data[off..off + lk_len].to_vec();
            off += lk_len;

            files.push(ManifestFileEntry {
                file_id,
                level,
                file_size,
                num_entries,
                smallest_key,
                largest_key,
            });
        }

        Ok(ManifestRecord {
            next_file_id,
            sequence_number,
            files,
        })
    }
}

/// Write manifest atomically: write to .tmp, fsync, rename.
pub fn write_manifest(dir: &Path, record: &ManifestRecord) -> Result<()> {
    let manifest_path = dir.join("MANIFEST");
    let tmp_path = dir.join("MANIFEST.tmp");

    let data = record.encode();

    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }

    fs::rename(&tmp_path, &manifest_path)?;
    Ok(())
}

/// Read manifest from disk. Returns None if no manifest exists.
pub fn read_manifest(dir: &Path) -> Result<Option<ManifestRecord>> {
    let manifest_path = dir.join("MANIFEST");
    if !manifest_path.exists() {
        return Ok(None);
    }

    let data = fs::read(&manifest_path)?;
    let record = ManifestRecord::decode(&data)?;
    Ok(Some(record))
}

fn crc32(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = generate_table();
    let mut crc = 0xffff_ffff_u32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xff) as usize;
        crc = (crc >> 8) ^ TABLE[idx];
    }
    !crc
}

const fn generate_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut j = 0;
        while j < 8 {
            if c & 1 != 0 {
                c = (c >> 1) ^ 0xedb8_8320;
            } else {
                c >>= 1;
            }
            j += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_roundtrip() {
        let record = ManifestRecord {
            next_file_id: 42,
            sequence_number: 100,
            files: vec![
                ManifestFileEntry {
                    file_id: 1,
                    level: 0,
                    file_size: 1024,
                    num_entries: 50,
                    smallest_key: b"aaa".to_vec(),
                    largest_key: b"zzz".to_vec(),
                },
                ManifestFileEntry {
                    file_id: 5,
                    level: 1,
                    file_size: 4096,
                    num_entries: 200,
                    smallest_key: b"abc".to_vec(),
                    largest_key: b"xyz".to_vec(),
                },
            ],
        };

        let encoded = record.encode();
        let decoded = ManifestRecord::decode(&encoded).unwrap();

        assert_eq!(decoded.next_file_id, 42);
        assert_eq!(decoded.sequence_number, 100);
        assert_eq!(decoded.files.len(), 2);
        assert_eq!(decoded.files[0].file_id, 1);
        assert_eq!(decoded.files[0].level, 0);
        assert_eq!(decoded.files[1].file_id, 5);
        assert_eq!(decoded.files[1].smallest_key, b"abc");
    }

    #[test]
    fn test_manifest_crc_corruption() {
        let record = ManifestRecord {
            next_file_id: 1,
            sequence_number: 0,
            files: vec![],
        };

        let mut encoded = record.encode();
        encoded[10] ^= 0xff; // corrupt a byte

        assert!(ManifestRecord::decode(&encoded).is_err());
    }

    #[test]
    fn test_manifest_write_read() {
        let dir = TempDir::new().unwrap();
        let record = ManifestRecord {
            next_file_id: 10,
            sequence_number: 50,
            files: vec![ManifestFileEntry {
                file_id: 3,
                level: 0,
                file_size: 2048,
                num_entries: 100,
                smallest_key: b"key_a".to_vec(),
                largest_key: b"key_z".to_vec(),
            }],
        };

        write_manifest(dir.path(), &record).unwrap();
        let loaded = read_manifest(dir.path()).unwrap().unwrap();

        assert_eq!(loaded.next_file_id, 10);
        assert_eq!(loaded.files.len(), 1);
        assert_eq!(loaded.files[0].file_id, 3);
    }

    #[test]
    fn test_manifest_empty() {
        let dir = TempDir::new().unwrap();
        assert!(read_manifest(dir.path()).unwrap().is_none());
    }
}
