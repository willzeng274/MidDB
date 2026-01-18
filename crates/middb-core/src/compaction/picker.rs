use super::version::{Version, VersionEdit};
use crate::config::Config;
use crate::sstable::SSTableMetadata;
use crate::Level;

#[derive(Debug)]
pub struct CompactionTask {
    pub level: Level,
    pub input_files: Vec<SSTableMetadata>,
    pub output_level: Level,
    pub target_files: Vec<SSTableMetadata>,
}

impl CompactionTask {
    pub fn all_input_files(&self) -> impl Iterator<Item = &SSTableMetadata> {
        self.input_files.iter().chain(self.target_files.iter())
    }

    pub fn to_edit(&self, output_file: SSTableMetadata) -> VersionEdit {
        let mut edit = VersionEdit::new();

        for file in &self.input_files {
            edit.delete_file(self.level, file.file_id);
        }
        for file in &self.target_files {
            edit.delete_file(self.output_level, file.file_id);
        }

        edit.add_file(self.output_level, output_file);
        edit
    }
}

pub struct CompactionPicker {
    level0_trigger: usize,
    level_size_base: u64,
    level_size_multiplier: u64,
}

impl CompactionPicker {
    pub fn new(config: &Config) -> Self {
        CompactionPicker {
            level0_trigger: config.level0_file_num_compaction_trigger,
            level_size_base: config.max_bytes_for_level_base,
            level_size_multiplier: config.max_bytes_for_level_multiplier,
        }
    }

    pub fn pick(&self, version: &Version) -> Option<CompactionTask> {
        if let Some(task) = self.pick_l0_compaction(version) {
            return Some(task);
        }

        for level in 1..6 {
            if let Some(task) = self.pick_level_compaction(version, level) {
                return Some(task);
            }
        }

        None
    }

    fn pick_l0_compaction(&self, version: &Version) -> Option<CompactionTask> {
        let l0 = version.level(0)?;

        if l0.file_count() < self.level0_trigger {
            return None;
        }

        let input_files: Vec<_> = l0.files.clone();

        let (smallest, largest) = Self::key_range(&input_files);
        let l1 = version.level(1)?;
        let target_files: Vec<_> = l1
            .find_overlapping(&smallest, &largest)
            .into_iter()
            .cloned()
            .collect();

        Some(CompactionTask {
            level: 0,
            input_files,
            output_level: 1,
            target_files,
        })
    }

    fn pick_level_compaction(&self, version: &Version, level: Level) -> Option<CompactionTask> {
        let level_files = version.level(level)?;
        let max_size = self.max_bytes_for_level(level);

        if level_files.total_size() <= max_size {
            return None;
        }

        let file = level_files.files.first()?.clone();

        let next_level = version.level(level + 1)?;
        let target_files: Vec<_> = next_level
            .find_overlapping(&file.smallest_key, &file.largest_key)
            .into_iter()
            .cloned()
            .collect();

        Some(CompactionTask {
            level,
            input_files: vec![file],
            output_level: level + 1,
            target_files,
        })
    }

    fn max_bytes_for_level(&self, level: Level) -> u64 {
        let mut size = self.level_size_base;
        for _ in 1..level {
            size *= self.level_size_multiplier;
        }
        size
    }

    fn key_range(files: &[SSTableMetadata]) -> (Vec<u8>, Vec<u8>) {
        let smallest = files
            .iter()
            .map(|f| &f.smallest_key)
            .min()
            .cloned()
            .unwrap_or_default();
        let largest = files
            .iter()
            .map(|f| &f.largest_key)
            .max()
            .cloned()
            .unwrap_or_default();
        (smallest, largest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::version::VersionSet;

    fn make_config() -> Config {
        let mut config = Config::default();
        config.level0_file_num_compaction_trigger = 4;
        config.max_bytes_for_level_base = 10 * 1024 * 1024;
        config
    }

    fn make_file(id: u64, smallest: &[u8], largest: &[u8], size: u64) -> SSTableMetadata {
        SSTableMetadata::new(id, size, smallest.to_vec(), largest.to_vec(), 100, 0)
    }

    #[test]
    fn test_no_compaction_needed() {
        let config = make_config();
        let picker = CompactionPicker::new(&config);
        let mut vs = VersionSet::new();

        vs.add_file(0, make_file(1, b"a", b"z", 1000));
        vs.add_file(0, make_file(2, b"a", b"z", 1000));

        let version = vs.current();
        assert!(picker.pick(&version).is_none());
    }

    #[test]
    fn test_l0_compaction_trigger() {
        let config = make_config();
        let picker = CompactionPicker::new(&config);
        let mut vs = VersionSet::new();

        for i in 0..4 {
            vs.add_file(0, make_file(i, b"a", b"z", 1000));
        }

        let version = vs.current();
        let task = picker.pick(&version);

        assert!(task.is_some());
        let task = task.unwrap();
        assert_eq!(task.level, 0);
        assert_eq!(task.output_level, 1);
        assert_eq!(task.input_files.len(), 4);
    }

    #[test]
    fn test_l0_compaction_with_overlap() {
        let config = make_config();
        let picker = CompactionPicker::new(&config);
        let mut vs = VersionSet::new();

        for i in 0..4 {
            vs.add_file(0, make_file(i, b"a", b"m", 1000));
        }
        vs.add_file(1, make_file(10, b"a", b"f", 1000));
        vs.add_file(1, make_file(11, b"g", b"m", 1000));
        vs.add_file(1, make_file(12, b"n", b"z", 1000));

        let version = vs.current();
        let task = picker.pick(&version).unwrap();

        assert_eq!(task.target_files.len(), 2);
    }

    #[test]
    fn test_version_edit_from_task() {
        let task = CompactionTask {
            level: 0,
            input_files: vec![make_file(1, b"a", b"z", 1000)],
            output_level: 1,
            target_files: vec![make_file(2, b"a", b"z", 1000)],
        };

        let output = make_file(3, b"a", b"z", 2000);
        let edit = task.to_edit(output);

        assert_eq!(edit.deleted_files.len(), 2);
        assert_eq!(edit.new_files.len(), 1);
    }
}
