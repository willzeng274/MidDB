use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Snappy = 2,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::None
    }
}

impl CompressionType {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Lz4),
            2 => Ok(CompressionType::Snappy),
            _ => Err(Error::Corruption(format!(
                "Unknown compression type: {}",
                value
            ))),
        }
    }
}

pub fn compress(data: &[u8], compression: CompressionType) -> Vec<u8> {
    match compression {
        CompressionType::None => {
            let mut out = Vec::with_capacity(1 + 4 + data.len());
            out.push(CompressionType::None as u8);
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(data);
            out
        }
        CompressionType::Lz4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            let mut out = Vec::with_capacity(1 + 4 + compressed.len());
            out.push(CompressionType::Lz4 as u8);
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
            out
        }
        CompressionType::Snappy => {
            let mut encoder = snap::raw::Encoder::new();
            let compressed = encoder.compress_vec(data).expect("snappy compression failed");
            let mut out = Vec::with_capacity(1 + 4 + compressed.len());
            out.push(CompressionType::Snappy as u8);
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
            out
        }
    }
}

pub fn decompress(data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 5 {
        return Err(Error::Corruption("Compressed block too short".into()));
    }

    let compression = CompressionType::from_u8(data[0])?;
    let uncompressed_size = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
    let payload = &data[5..];

    match compression {
        CompressionType::None => {
            if payload.len() != uncompressed_size {
                return Err(Error::Corruption("Uncompressed size mismatch".into()));
            }
            Ok(payload.to_vec())
        }
        CompressionType::Lz4 => {
            let decompressed = lz4_flex::decompress_size_prepended(payload)
                .map_err(|e| Error::Corruption(format!("LZ4 decompression failed: {}", e)))?;
            if decompressed.len() != uncompressed_size {
                return Err(Error::Corruption("LZ4 uncompressed size mismatch".into()));
            }
            Ok(decompressed)
        }
        CompressionType::Snappy => {
            let mut decoder = snap::raw::Decoder::new();
            let decompressed = decoder
                .decompress_vec(payload)
                .map_err(|e| Error::Corruption(format!("Snappy decompression failed: {}", e)))?;
            if decompressed.len() != uncompressed_size {
                return Err(Error::Corruption(
                    "Snappy uncompressed size mismatch".into(),
                ));
            }
            Ok(decompressed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_roundtrip() {
        let data = b"hello world this is uncompressed data";
        let compressed = compress(data, CompressionType::None);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"hello world ".repeat(100);
        let compressed = compress(&data, CompressionType::Lz4);
        assert!(compressed.len() < data.len());
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_roundtrip() {
        let data = b"hello world ".repeat(100);
        let compressed = compress(&data, CompressionType::Snappy);
        assert!(compressed.len() < data.len());
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_type_from_u8() {
        assert_eq!(CompressionType::from_u8(0).unwrap(), CompressionType::None);
        assert_eq!(CompressionType::from_u8(1).unwrap(), CompressionType::Lz4);
        assert_eq!(
            CompressionType::from_u8(2).unwrap(),
            CompressionType::Snappy
        );
        assert!(CompressionType::from_u8(99).is_err());
    }

    #[test]
    fn test_empty_data() {
        for ct in [
            CompressionType::None,
            CompressionType::Lz4,
            CompressionType::Snappy,
        ] {
            let compressed = compress(b"", ct);
            let decompressed = decompress(&compressed).unwrap();
            assert!(decompressed.is_empty());
        }
    }
}
