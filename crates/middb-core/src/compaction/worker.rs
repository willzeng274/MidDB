use super::picker::{CompactionPicker, CompactionTask};
use super::version::VersionSet;
use crate::config::Config;
use crate::sstable::{MergeIterator, SSTableReader, SSTableWriter};
use crate::Result;
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
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
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            Self::run_loop(version_set, readers, config, shutdown_clone);
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
    ) {
        let picker = CompactionPicker::new(&config);

        while !shutdown.load(Ordering::SeqCst) {
            let task = {
                let vs = version_set.read().unwrap();
                let version = vs.current();
                picker.pick(&version)
            };

            if let Some(task) = task {
                if let Err(e) = Self::run_compaction(&task, &version_set, &readers, &config) {
                    eprintln!("compaction failed: {}", e);
                }
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    fn run_compaction(
        task: &CompactionTask,
        version_set: &Arc<RwLock<VersionSet>>,
        readers: &Arc<RwLock<HashMap<u64, SSTableReader>>>,
        config: &Config,
    ) -> Result<()> {
        let file_id = {
            let vs = version_set.read().unwrap();
            vs.next_file_id()
        };

        let output_path = config.data_dir.join(format!("sst_{:08}.sst", file_id));
        
        let iters = {
            let readers_guard = readers.read().unwrap();
            let mut iters = Vec::new();

            for file in task.all_input_files() {
                if let Some(reader) = readers_guard.get(&file.file_id) {
                    iters.push(reader.iter()?);
                }
            }
            iters
        };

        let mut merge_iter = MergeIterator::new(iters);
        merge_iter.seek_to_first()?;

        let mut writer = SSTableWriter::create(&output_path, config.block_size)?;

        while merge_iter.valid() {
            if let (Some(key), Some(value)) = (merge_iter.key(), merge_iter.value()) {
                writer.add(key, value)?;
            }
            merge_iter.next()?;
        }

        let metadata = writer.finish(file_id, task.output_level)?;

        let new_reader = SSTableReader::open(&output_path)?;
        {
            let mut readers_guard = readers.write().unwrap();
            readers_guard.insert(file_id, new_reader);
        }

        let edit = task.to_edit(metadata);
        {
            let mut vs = version_set.write().unwrap();
            vs.apply_edit(edit);
        }

        {
            let mut readers_guard = readers.write().unwrap();
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
            let vs = self.version_set.read().unwrap();
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
        let path = dir.path().join(format!("sst_{:08}.sst", id));
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

        let vs = version_set.read().unwrap();
        assert_eq!(vs.l0_file_count(), 0);
        assert_eq!(vs.current().level(1).unwrap().file_count(), 1);
    }
}
