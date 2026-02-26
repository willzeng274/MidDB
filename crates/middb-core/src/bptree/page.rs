use crate::storage::page::{Page, PAGE_SIZE};
use crate::{Error, Result};

pub const NODE_TYPE_LEAF: u8 = 1;
pub const NODE_TYPE_INTERIOR: u8 = 2;

const LEAF_HEADER_SIZE: usize = 17;
const INTERIOR_HEADER_SIZE: usize = 9;
const SLOT_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct LeafPageData {
    pub page_id: u32,
    pub next_leaf: u32,
    pub prev_leaf: u32,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct InteriorPageData {
    pub page_id: u32,
    pub keys: Vec<Vec<u8>>,
    pub children: Vec<u32>,
}

pub fn encode_leaf_page(data: &LeafPageData) -> Result<Page> {
    let mut page = Page::new();
    let buf = page.data_mut();

    buf[0] = NODE_TYPE_LEAF;
    let count = data.entries.len() as u16;
    buf[1..3].copy_from_slice(&count.to_le_bytes());
    buf[3..7].copy_from_slice(&data.page_id.to_le_bytes());
    buf[7..11].copy_from_slice(&data.next_leaf.to_le_bytes());
    buf[11..15].copy_from_slice(&data.prev_leaf.to_le_bytes());

    let slots_end = LEAF_HEADER_SIZE + 2 + count as usize * SLOT_SIZE;
    let mut data_cursor = PAGE_SIZE;

    let mut slot_offsets = Vec::with_capacity(count as usize);

    for (key, value) in &data.entries {
        let entry_size = 2 + key.len() + 2 + value.len();
        if data_cursor < slots_end + entry_size {
            return Err(Error::Internal("Leaf page overflow".into()));
        }
        data_cursor -= entry_size;
        slot_offsets.push(data_cursor as u16);

        buf[data_cursor..data_cursor + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        buf[data_cursor + 2..data_cursor + 2 + key.len()].copy_from_slice(key);
        let val_off = data_cursor + 2 + key.len();
        buf[val_off..val_off + 2].copy_from_slice(&(value.len() as u16).to_le_bytes());
        buf[val_off + 2..val_off + 2 + value.len()].copy_from_slice(value);
    }

    buf[15..17].copy_from_slice(&(data_cursor as u16).to_le_bytes());

    let mut off = LEAF_HEADER_SIZE + 2;
    for &slot in &slot_offsets {
        buf[off..off + 2].copy_from_slice(&slot.to_le_bytes());
        off += SLOT_SIZE;
    }

    Ok(page)
}

pub fn decode_leaf_page(page: &Page) -> Result<LeafPageData> {
    let buf = page.data();

    if buf[0] != NODE_TYPE_LEAF {
        return Err(Error::Corruption(format!(
            "Expected leaf page type {}, got {}",
            NODE_TYPE_LEAF, buf[0]
        )));
    }

    let count = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    let page_id = u32::from_le_bytes(buf[3..7].try_into().unwrap());
    let next_leaf = u32::from_le_bytes(buf[7..11].try_into().unwrap());
    let prev_leaf = u32::from_le_bytes(buf[11..15].try_into().unwrap());

    let mut entries = Vec::with_capacity(count);
    let slot_base = LEAF_HEADER_SIZE + 2;

    for i in 0..count {
        let slot_off = slot_base + i * SLOT_SIZE;
        let data_off = u16::from_le_bytes([buf[slot_off], buf[slot_off + 1]]) as usize;

        let key_len = u16::from_le_bytes([buf[data_off], buf[data_off + 1]]) as usize;
        let key = buf[data_off + 2..data_off + 2 + key_len].to_vec();
        let val_off = data_off + 2 + key_len;
        let val_len = u16::from_le_bytes([buf[val_off], buf[val_off + 1]]) as usize;
        let value = buf[val_off + 2..val_off + 2 + val_len].to_vec();

        entries.push((key, value));
    }

    Ok(LeafPageData {
        page_id,
        next_leaf,
        prev_leaf,
        entries,
    })
}

pub fn encode_interior_page(data: &InteriorPageData) -> Result<Page> {
    let mut page = Page::new();
    let buf = page.data_mut();

    buf[0] = NODE_TYPE_INTERIOR;
    let count = data.keys.len() as u16;
    buf[1..3].copy_from_slice(&count.to_le_bytes());
    buf[3..7].copy_from_slice(&data.page_id.to_le_bytes());

    let children_start = INTERIOR_HEADER_SIZE;
    let children_size = data.children.len() * 4;
    for (i, &child_id) in data.children.iter().enumerate() {
        let off = children_start + i * 4;
        buf[off..off + 4].copy_from_slice(&child_id.to_le_bytes());
    }

    let slots_start = children_start + children_size;
    let slots_size = count as usize * SLOT_SIZE;
    let mut data_cursor = PAGE_SIZE;

    let mut slot_offsets = Vec::with_capacity(count as usize);

    for key in &data.keys {
        let entry_size = 2 + key.len();
        if data_cursor < slots_start + slots_size + entry_size {
            return Err(Error::Internal("Interior page overflow".into()));
        }
        data_cursor -= entry_size;
        slot_offsets.push(data_cursor as u16);

        buf[data_cursor..data_cursor + 2].copy_from_slice(&(key.len() as u16).to_le_bytes());
        buf[data_cursor + 2..data_cursor + 2 + key.len()].copy_from_slice(key);
    }

    buf[7..9].copy_from_slice(&(data_cursor as u16).to_le_bytes());

    for (i, &slot) in slot_offsets.iter().enumerate() {
        let off = slots_start + i * SLOT_SIZE;
        buf[off..off + 2].copy_from_slice(&slot.to_le_bytes());
    }

    Ok(page)
}

pub fn decode_interior_page(page: &Page) -> Result<InteriorPageData> {
    let buf = page.data();

    if buf[0] != NODE_TYPE_INTERIOR {
        return Err(Error::Corruption(format!(
            "Expected interior page type {}, got {}",
            NODE_TYPE_INTERIOR, buf[0]
        )));
    }

    let count = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    let page_id = u32::from_le_bytes(buf[3..7].try_into().unwrap());

    let num_children = count + 1;
    let children_start = INTERIOR_HEADER_SIZE;
    let mut children = Vec::with_capacity(num_children);
    for i in 0..num_children {
        let off = children_start + i * 4;
        children.push(u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()));
    }

    let slots_start = children_start + num_children * 4;
    let mut keys = Vec::with_capacity(count);
    for i in 0..count {
        let slot_off = slots_start + i * SLOT_SIZE;
        let data_off = u16::from_le_bytes([buf[slot_off], buf[slot_off + 1]]) as usize;
        let key_len = u16::from_le_bytes([buf[data_off], buf[data_off + 1]]) as usize;
        let key = buf[data_off + 2..data_off + 2 + key_len].to_vec();
        keys.push(key);
    }

    Ok(InteriorPageData {
        page_id,
        keys,
        children,
    })
}

pub fn get_page_type(page: &Page) -> u8 {
    page.data()[0]
}

pub fn leaf_entry_capacity(avg_key_size: usize, avg_value_size: usize) -> usize {
    let per_entry = SLOT_SIZE + 2 + avg_key_size + 2 + avg_value_size;
    let available = PAGE_SIZE - LEAF_HEADER_SIZE - 2;
    available / per_entry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leaf_page_roundtrip() {
        let data = LeafPageData {
            page_id: 42,
            next_leaf: 43,
            prev_leaf: 41,
            entries: vec![
                (b"alpha".to_vec(), b"value1".to_vec()),
                (b"beta".to_vec(), b"value2".to_vec()),
                (b"gamma".to_vec(), b"value3".to_vec()),
            ],
        };

        let page = encode_leaf_page(&data).unwrap();
        let decoded = decode_leaf_page(&page).unwrap();

        assert_eq!(decoded.page_id, 42);
        assert_eq!(decoded.next_leaf, 43);
        assert_eq!(decoded.prev_leaf, 41);
        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[0], (b"alpha".to_vec(), b"value1".to_vec()));
        assert_eq!(decoded.entries[1], (b"beta".to_vec(), b"value2".to_vec()));
        assert_eq!(decoded.entries[2], (b"gamma".to_vec(), b"value3".to_vec()));
    }

    #[test]
    fn test_interior_page_roundtrip() {
        let data = InteriorPageData {
            page_id: 10,
            keys: vec![b"middle".to_vec(), b"zeta".to_vec()],
            children: vec![1, 2, 3],
        };

        let page = encode_interior_page(&data).unwrap();
        let decoded = decode_interior_page(&page).unwrap();

        assert_eq!(decoded.page_id, 10);
        assert_eq!(decoded.keys, vec![b"middle".to_vec(), b"zeta".to_vec()]);
        assert_eq!(decoded.children, vec![1, 2, 3]);
    }

    #[test]
    fn test_empty_leaf_page() {
        let data = LeafPageData {
            page_id: 0,
            next_leaf: 0,
            prev_leaf: 0,
            entries: vec![],
        };

        let page = encode_leaf_page(&data).unwrap();
        let decoded = decode_leaf_page(&page).unwrap();
        assert_eq!(decoded.entries.len(), 0);
    }

    #[test]
    fn test_page_type_detection() {
        let leaf = encode_leaf_page(&LeafPageData {
            page_id: 0,
            next_leaf: 0,
            prev_leaf: 0,
            entries: vec![],
        })
        .unwrap();
        assert_eq!(get_page_type(&leaf), NODE_TYPE_LEAF);

        let interior = encode_interior_page(&InteriorPageData {
            page_id: 0,
            keys: vec![b"k".to_vec()],
            children: vec![1, 2],
        })
        .unwrap();
        assert_eq!(get_page_type(&interior), NODE_TYPE_INTERIOR);
    }

    #[test]
    fn test_leaf_capacity() {
        let cap = leaf_entry_capacity(16, 64);
        assert!(cap > 0);
        assert!(cap < 200);
    }
}
