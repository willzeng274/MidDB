use super::picker::{CompactionPicker, CompactionTask};
use super::version::VersionSet;
use crate::cache::BlockCache;
use crate::config::Config;
use crate::manifest::{self, ManifestFileEntry, ManifestRecord};
use crate::sstable::{MergeIterator, SSTableReader, SSTableWriter};
use crate::Result;
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use parking_lot::RwLock;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub struct CompactionWorker {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl CompactionWorker {
    pub fn start(
        version_set: Arc<RwLock<VersionSet>>,
        readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
        config: Config,
        block_cache: Arc<BlockCache>,
    ) -> Self {
        Self::start_with_sequence(version_set, readers, config, block_cache, Arc::new(AtomicU64::new(0)))
    }

    pub fn start_with_sequence(
        version_set: Arc<RwLock<VersionSet>>,
        readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
        config: Config,
        block_cache: Arc<BlockCache>,
        sequence: Arc<AtomicU64>,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            Self::run_loop(version_set, readers, config, shutdown_clone, block_cache, sequence);
        });

        CompactionWorker {
            handle: Some(handle),
            shutdown,
        }
    }

    pub fn stop(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    fn run_loop(
        version_set: Arc<RwLock<VersionSet>>,
        readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
        config: Config,
        shutdown: Arc<AtomicBool>,
        block_cache: Arc<BlockCache>,
        sequence: Arc<AtomicU64>,
    ) {
        let picker = CompactionPicker::new(&config);

        while !shutdown.load(Ordering::SeqCst) {
            // Keep compacting as long as there's work — don't sleep between consecutive
            // compactions. This prevents L0 file accumulation during heavy write bursts.
            let mut did_work = true;
            while did_work && !shutdown.load(Ordering::Relaxed) {
                let task = {
                    let vs = version_set.read();
                    let version = vs.current();
                    picker.pick(&version)
                };

                match task {
                    Some(task) => {
                        if let Err(_e) = Self::run_compaction(
                            &task, &version_set, &readers, &config, &block_cache, &sequence,
                        ) {
                            // Compaction failure is non-fatal — will retry on next cycle.
                            // Common cause: stale task referencing files from a concurrent flush.
                            did_work = false;
                        }
                    }
                    None => {
                        did_work = false;
                    }
                }
            }

            thread::sleep(Duration::from_millis(10));
        }
    }

    fn persist_manifest_static(
        version_set: &Arc<RwLock<VersionSet>>,
        config: &Config,
        sequence: &Arc<AtomicU64>,
    ) -> Result<()> {
        let vs = version_set.read();
        let version = vs.current();
        let files: Vec<ManifestFileEntry> = version.all_files()
            .map(ManifestFileEntry::from_metadata)
            .collect();

        let record = ManifestRecord {
            next_file_id: vs.current_file_id(),
            sequence_number: sequence.load(Ordering::SeqCst),
            files,
        };
        manifest::write_manifest(&config.data_dir, &record)
    }

    fn run_compaction(
        task: &CompactionTask,
        version_set: &Arc<RwLock<VersionSet>>,
        readers: &Arc<RwLock<HashMap<u64, SSTableReader>>>,
        config: &Config,
        block_cache: &Arc<BlockCache>,
        sequence: &Arc<AtomicU64>,
    ) -> Result<()> {
        let file_id = {
            let vs = version_set.read();
            vs.next_file_id()
        };

        let output_path = config.data_dir.join(format!("sst_{file_id:08}.sst"));

        if !output_path.parent().is_some_and(|p| p.exists()) {
            return Ok(()); // data directory gone (DB shutting down)
        }

        let iters = {
            let readers_guard = readers.read();
            let mut iters = Vec::new();

            // Input files are ordered oldest→newest. Reverse so that newer files
            // get lower iterator indices. MergeIterator gives priority to the
            // lowest index on key ties, so this ensures the newest value wins.
            let ordered_files: Vec<_> = task.input_files.iter().rev()
                .chain(task.target_files.iter())
                .collect();

            for file in &ordered_files {
                match readers_guard.get(&file.file_id) {
                    Some(reader) => iters.push(reader.iter()?),
                    None => return Ok(()), // stale task — file already compacted away
                }
            }
            iters
        };

        let mut merge_iter = MergeIterator::new(iters);
        merge_iter.seek_to_first()?;

        let mut writer = SSTableWriter::create_with_options(
            &output_path, config.block_size, config.bloom_bits_per_key, config.compression_type,
        )?;

        // Only drop tombstones when compacting to the maximum level (6).
        // This avoids a TOCTOU race: a concurrent flush could add files to a
        // lower level between our check and the tombstone drop decision.
        // At level 6 there's nothing below by definition, so it's always safe.
        let is_bottom_level = task.output_level >= 6;

        while merge_iter.valid() {
            if let (Some(key), Some(value)) = (merge_iter.key(), merge_iter.value()) {
                let is_tombstone = value.len() == 1 && value[0] == 0x02;
                if is_tombstone && is_bottom_level {
                    merge_iter.next()?;
                    continue;
                }
                writer.add(key, value)?;
            }
            merge_iter.next()?;
        }

        let metadata = writer.finish(file_id, task.output_level)?;

        let new_reader = SSTableReader::open_with_cache(
            &output_path, file_id, Some(Arc::clone(block_cache)),
        )?;
        {
            let mut readers_guard = readers.write();
            readers_guard.insert(file_id, new_reader);
        }

        let edit = task.to_edit(metadata);
        {
            let mut vs = version_set.write();
            vs.apply_edit(edit);
        }

        // Persist manifest before deleting old files — crash safety
        Self::persist_manifest_static(version_set, config, sequence)?;

        {
            let mut readers_guard = readers.write();
            for file in task.all_input_files() {
                readers_guard.remove(&file.file_id);
            }
        }

        for file in task.all_input_files() {
            let path = config.data_dir.join(format!("sst_{:08}.sst", file.file_id));
            let _ = fs::remove_file(path);
        }

        Ok(())
    }
}

impl Drop for CompactionWorker {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub struct CompactionRunner {
    version_set: Arc<RwLock<VersionSet>>,
    readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
    config: Config,
    picker: CompactionPicker,
}

impl CompactionRunner {
    pub fn new(
        version_set: Arc<RwLock<VersionSet>>,
        readers: Arc<RwLock<HashMap<u64, SSTableReader>>>,
        config: Config,
    ) -> Self {
        let picker = CompactionPicker::new(&config);
        CompactionRunner {
            version_set,
            readers,
            config,
            picker,
        }
    }

    pub fn maybe_compact(&self) -> Result<bool> {
        let task = {
            let vs = self.version_set.read();
            let version = vs.current();
            self.picker.pick(&version)
        };

        match task {
            Some(task) => {
                CompactionWorker::run_compaction(
                    &task,
                    &self.version_set,
                    &self.readers,
                    &self.config,
                    &Arc::new(crate::cache::BlockCache::new(0)),
                    &Arc::new(AtomicU64::new(0)),
                )?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::version::VersionSet;
    use crate::sstable::SSTableMetadata;
    use tempfile::TempDir;

    fn setup_test_sstable(dir: &TempDir, id: u64, data: &[(Vec<u8>, Vec<u8>)]) -> SSTableMetadata {
        let path = dir.path().join(format!("sst_{id:08}.sst"));
        let mut writer = SSTableWriter::create(&path, 4096).unwrap();

        for (k, v) in data {
            writer.add(k, v).unwrap();
        }

        writer.finish(id, 0).unwrap()
    }

    #[test]
    fn test_compaction_runner() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = Config::new(temp_dir.path());
        config.level0_file_num_compaction_trigger = 2;

        fs::create_dir_all(&config.data_dir).unwrap();

        let mut vs = VersionSet::new();
        let mut readers = HashMap::new();

        let m1 = setup_test_sstable(&temp_dir, 1, &[(b"a".to_vec(), b"1".to_vec())]);
        let m2 = setup_test_sstable(&temp_dir, 2, &[(b"b".to_vec(), b"2".to_vec())]);

        readers.insert(1, SSTableReader::open(temp_dir.path().join("sst_00000001.sst")).unwrap());
        readers.insert(2, SSTableReader::open(temp_dir.path().join("sst_00000002.sst")).unwrap());

        vs.add_file(0, m1);
        vs.add_file(0, m2);

        let version_set = Arc::new(RwLock::new(vs));
        let readers = Arc::new(RwLock::new(readers));

        let runner = CompactionRunner::new(
            Arc::clone(&version_set),
            Arc::clone(&readers),
            config,
        );

        let compacted = runner.maybe_compact().unwrap();
        assert!(compacted);

        let vs = version_set.read();
        assert_eq!(vs.l0_file_count(), 0);
        assert_eq!(vs.current().level(1).unwrap().file_count(), 1);
    }
}
