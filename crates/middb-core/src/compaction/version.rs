use crate::sstable::SSTableMetadata;
use crate::Level;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const MAX_LEVELS: usize = 7;

#[derive(Debug, Clone)]
pub struct LevelFiles {
    pub level: Level,
    pub files: Vec<SSTableMetadata>,
}

impl LevelFiles {
    pub fn new(level: Level) -> Self {
        LevelFiles {
            level,
            files: Vec::new(),
        }
    }

    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.file_size).sum()
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub fn add_file(&mut self, file: SSTableMetadata) {
        if self.level == 0 {
            self.files.push(file);
        } else {
            let pos = self
                .files
                .binary_search_by(|f| f.smallest_key.cmp(&file.smallest_key))
                .unwrap_or_else(|i| i);
            self.files.insert(pos, file);
        }
    }

    pub fn remove_file(&mut self, file_id: u64) {
        self.files.retain(|f| f.file_id != file_id);
    }

    pub fn find_overlapping(&self, smallest: &[u8], largest: &[u8]) -> Vec<&SSTableMetadata> {
        self.files
            .iter()
            .filter(|f| Self::ranges_overlap(&f.smallest_key, &f.largest_key, smallest, largest))
            .collect()
    }

    fn ranges_overlap(a_min: &[u8], a_max: &[u8], b_min: &[u8], b_max: &[u8]) -> bool {
        a_min <= b_max && b_min <= a_max
    }
}

#[derive(Debug, Clone)]
pub struct Version {
    pub levels: Vec<LevelFiles>,
}

impl Version {
    pub fn new() -> Self {
        let levels = (0..MAX_LEVELS as u32)
            .map(|i| LevelFiles::new(i))
            .collect();
        Version { levels }
    }

    pub fn level(&self, level: Level) -> Option<&LevelFiles> {
        self.levels.get(level as usize)
    }

    pub fn level_mut(&mut self, level: Level) -> Option<&mut LevelFiles> {
        self.levels.get_mut(level as usize)
    }

    pub fn l0_file_count(&self) -> usize {
        self.levels.first().map(|l| l.file_count()).unwrap_or(0)
    }

    pub fn level_size(&self, level: Level) -> u64 {
        self.levels
            .get(level as usize)
            .map(|l| l.total_size())
            .unwrap_or(0)
    }

    pub fn all_files(&self) -> impl Iterator<Item = &SSTableMetadata> {
        self.levels.iter().flat_map(|l| l.files.iter())
    }

    pub fn files_for_key(&self, key: &[u8]) -> Vec<&SSTableMetadata> {
        let mut result = Vec::new();

        if let Some(l0) = self.levels.first() {
            for file in l0.files.iter().rev() {
                if file.may_contain(key) {
                    result.push(file);
                }
            }
        }

        for level in self.levels.iter().skip(1) {
            let pos = level
                .files
                .binary_search_by(|f| {
                    if key < f.smallest_key.as_slice() {
                        std::cmp::Ordering::Greater
                    } else if key > f.largest_key.as_slice() {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
                .ok();

            if let Some(idx) = pos {
                result.push(&level.files[idx]);
            }
        }

        result
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

pub struct VersionSet {
    current: Arc<Version>,
    next_file_id: AtomicU64,
}

impl VersionSet {
    pub fn new() -> Self {
        VersionSet {
            current: Arc::new(Version::new()),
            next_file_id: AtomicU64::new(1),
        }
    }

    pub fn current(&self) -> Arc<Version> {
        Arc::clone(&self.current)
    }

    pub fn next_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn add_file(&mut self, level: Level, file: SSTableMetadata) {
        let mut new_version = (*self.current).clone();
        if let Some(level_files) = new_version.level_mut(level) {
            level_files.add_file(file);
        }
        self.current = Arc::new(new_version);
    }

    pub fn apply_edit(&mut self, edit: VersionEdit) {
        let mut new_version = (*self.current).clone();

        for (level, file_id) in edit.deleted_files {
            if let Some(level_files) = new_version.level_mut(level) {
                level_files.remove_file(file_id);
            }
        }

        for (level, file) in edit.new_files {
            if let Some(level_files) = new_version.level_mut(level) {
                level_files.add_file(file);
            }
        }

        self.current = Arc::new(new_version);
    }

    pub fn l0_file_count(&self) -> usize {
        self.current.l0_file_count()
    }

    pub fn level_size(&self, level: Level) -> u64 {
        self.current.level_size(level)
    }
}

impl Default for VersionSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct VersionEdit {
    pub deleted_files: Vec<(Level, u64)>,
    pub new_files: Vec<(Level, SSTableMetadata)>,
}

impl VersionEdit {
    pub fn new() -> Self {
        VersionEdit {
            deleted_files: Vec::new(),
            new_files: Vec::new(),
        }
    }

    pub fn delete_file(&mut self, level: Level, file_id: u64) {
        self.deleted_files.push((level, file_id));
    }

    pub fn add_file(&mut self, level: Level, file: SSTableMetadata) {
        self.new_files.push((level, file));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file(id: u64, smallest: &[u8], largest: &[u8]) -> SSTableMetadata {
        SSTableMetadata::new(id, 1000, smallest.to_vec(), largest.to_vec(), 100, 0)
    }

    #[test]
    fn test_version_new() {
        let v = Version::new();
        assert_eq!(v.levels.len(), MAX_LEVELS);
        assert_eq!(v.l0_file_count(), 0);
    }

    #[test]
    fn test_level_files_add() {
        let mut level = LevelFiles::new(1);
        level.add_file(make_file(1, b"b", b"d"));
        level.add_file(make_file(2, b"a", b"a"));
        level.add_file(make_file(3, b"e", b"f"));

        assert_eq!(level.files[0].file_id, 2);
        assert_eq!(level.files[1].file_id, 1);
        assert_eq!(level.files[2].file_id, 3);
    }

    #[test]
    fn test_find_overlapping() {
        let mut level = LevelFiles::new(1);
        level.add_file(make_file(1, b"a", b"c"));
        level.add_file(make_file(2, b"d", b"f"));
        level.add_file(make_file(3, b"g", b"i"));

        let overlapping = level.find_overlapping(b"b", b"e");
        assert_eq!(overlapping.len(), 2);
    }

    #[test]
    fn test_version_set_add_file() {
        let mut vs = VersionSet::new();
        vs.add_file(0, make_file(1, b"a", b"z"));

        assert_eq!(vs.l0_file_count(), 1);
    }

    #[test]
    fn test_version_edit() {
        let mut vs = VersionSet::new();
        vs.add_file(0, make_file(1, b"a", b"z"));
        vs.add_file(0, make_file(2, b"b", b"y"));

        let mut edit = VersionEdit::new();
        edit.delete_file(0, 1);
        edit.add_file(1, make_file(3, b"a", b"z"));

        vs.apply_edit(edit);

        assert_eq!(vs.l0_file_count(), 1);
        assert_eq!(vs.current.level(1).unwrap().file_count(), 1);
    }

    #[test]
    fn test_files_for_key() {
        let mut vs = VersionSet::new();
        vs.add_file(0, make_file(1, b"a", b"m"));
        vs.add_file(0, make_file(2, b"k", b"z"));
        vs.add_file(1, make_file(3, b"a", b"f"));
        vs.add_file(1, make_file(4, b"g", b"m"));
        vs.add_file(1, make_file(5, b"n", b"z"));

        let version = vs.current();
        let files = version.files_for_key(b"d");

        assert!(files.iter().any(|f| f.file_id == 1));
        assert!(files.iter().any(|f| f.file_id == 3));
    }
}
