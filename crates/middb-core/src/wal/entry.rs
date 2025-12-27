use crate::{Error, Result, SequenceNumber};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryType {
    Put = 1,
    Delete = 2,
}

impl EntryType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(EntryType::Put),
            2 => Ok(EntryType::Delete),
            _ => Err(Error::Corruption(format!("Invalid entry type: {}", value))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub sequence_number: SequenceNumber,
    pub entry_type: EntryType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl WalEntry {
    pub fn put(sequence_number: SequenceNumber, key: Vec<u8>, value: Vec<u8>) -> Self {
        WalEntry {
            sequence_number,
            entry_type: EntryType::Put,
            key,
            value: Some(value),
        }
    }
    
    pub fn delete(sequence_number: SequenceNumber, key: Vec<u8>) -> Self {
        WalEntry {
            sequence_number,
            entry_type: EntryType::Delete,
            key,
            value: None,
        }
    }
    
    pub fn encode(&self) -> Vec<u8> {
        let key_len = self.key.len() as u32;
        let value_len = self.value.as_ref().map_or(0, |v| v.len()) as u32;
        
        let data_len = 8 + 1 + 4 + key_len + 4 + value_len;
        let mut buf = Vec::with_capacity(8 + data_len as usize);
        
        buf.extend_from_slice(&[0u8; 8]);
        
        buf.extend_from_slice(&self.sequence_number.to_le_bytes());
        buf.push(self.entry_type as u8);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&value_len.to_le_bytes());
        if let Some(ref value) = self.value {
            buf.extend_from_slice(value);
        }
        
        let crc = crc32(&buf[8..]);
        
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf[4..8].copy_from_slice(&data_len.to_le_bytes());
        
        buf
    }
    
    pub fn decode(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 8 {
            return Err(Error::Corruption("WAL entry too short".to_string()));
        }
        
        let crc = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let data_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        
        if data.len() < 8 + data_len {
            return Err(Error::Corruption("WAL entry incomplete".to_string()));
        }
        
        let entry_data = &data[8..8 + data_len];
        
        let computed_crc = crc32(entry_data);
        if crc != computed_crc {
            return Err(Error::Corruption(format!(
                "WAL entry CRC mismatch: expected {:#x}, got {:#x}",
                crc, computed_crc
            )));
        }
        
        let mut offset = 0;
        
        if offset + 8 > entry_data.len() {
            return Err(Error::Corruption("Invalid sequence number".to_string()));
        }
        let sequence_number = u64::from_le_bytes(
            entry_data[offset..offset + 8].try_into().unwrap()
        );
        offset += 8;
        
        if offset >= entry_data.len() {
            return Err(Error::Corruption("Invalid entry type".to_string()));
        }
        let entry_type = EntryType::from_u8(entry_data[offset])?;
        offset += 1;
        
        if offset + 4 > entry_data.len() {
            return Err(Error::Corruption("Invalid key length".to_string()));
        }
        let key_len = u32::from_le_bytes(
            entry_data[offset..offset + 4].try_into().unwrap()
        ) as usize;
        offset += 4;
        
        if offset + key_len > entry_data.len() {
            return Err(Error::Corruption("Invalid key data".to_string()));
        }
        let key = entry_data[offset..offset + key_len].to_vec();
        offset += key_len;
        
        if offset + 4 > entry_data.len() {
            return Err(Error::Corruption("Invalid value length".to_string()));
        }
        let value_len = u32::from_le_bytes(
            entry_data[offset..offset + 4].try_into().unwrap()
        ) as usize;
        offset += 4;
        
        let value = if value_len > 0 {
            if offset + value_len > entry_data.len() {
                return Err(Error::Corruption("Invalid value data".to_string()));
            }
            Some(entry_data[offset..offset + value_len].to_vec())
        } else {
            None
        };
        
        Ok((
            WalEntry {
                sequence_number,
                entry_type,
                key,
                value,
            },
            8 + data_len,
        ))
    }
}

fn crc32(data: &[u8]) -> u32 {
    const CRC32_TABLE: &[u32] = &generate_crc32_table();
    
    let mut crc = 0xffff_ffff;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    !crc
}

const fn generate_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_put_entry_encode_decode() {
        let entry = WalEntry::put(42, b"mykey".to_vec(), b"myvalue".to_vec());
        let encoded = entry.encode();
        let (decoded, size) = WalEntry::decode(&encoded).unwrap();
        
        assert_eq!(decoded.sequence_number, 42);
        assert_eq!(decoded.entry_type, EntryType::Put);
        assert_eq!(decoded.key, b"mykey");
        assert_eq!(decoded.value, Some(b"myvalue".to_vec()));
        assert_eq!(size, encoded.len());
    }
    
    #[test]
    fn test_delete_entry_encode_decode() {
        let entry = WalEntry::delete(100, b"deleteme".to_vec());
        let encoded = entry.encode();
        let (decoded, size) = WalEntry::decode(&encoded).unwrap();
        
        assert_eq!(decoded.sequence_number, 100);
        assert_eq!(decoded.entry_type, EntryType::Delete);
        assert_eq!(decoded.key, b"deleteme");
        assert_eq!(decoded.value, None);
        assert_eq!(size, encoded.len());
    }
    
    #[test]
    fn test_corrupted_crc() {
        let entry = WalEntry::put(1, b"key".to_vec(), b"value".to_vec());
        let mut encoded = entry.encode();
        
        // Corrupt a byte
        encoded[10] ^= 0xff;
        
        let result = WalEntry::decode(&encoded);
        assert!(result.is_err());
    }
}
