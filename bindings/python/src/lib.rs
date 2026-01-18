use middb_core::{Config, Database as CoreDatabase};
use pyo3::exceptions::{PyIOError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::path::PathBuf;
use std::sync::Arc;

#[pyclass]
struct Database {
    db: Option<Arc<CoreDatabase>>,
}

#[pymethods]
impl Database {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let config = Config::new(PathBuf::from(path));
        let db = CoreDatabase::open(config)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to open database: {}", e)))?;
        
        Ok(Database { db: Some(Arc::new(db)) })
    }
    
    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        let db = self.db.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Database is closed"))?;
        
        db.put(key.to_vec(), value.to_vec())
            .map_err(|e| PyIOError::new_err(format!("Put failed: {}", e)))
    }
    
    fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let db = self.db.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Database is closed"))?;
        
        match db.get(&key.to_vec()) {
            Ok(Some(value)) => Ok(Some(PyBytes::new_bound(py, &value))),
            Ok(None) => Ok(None),
            Err(e) => Err(PyIOError::new_err(format!("Get failed: {}", e))),
        }
    }
    
    fn delete(&self, key: &[u8]) -> PyResult<()> {
        let db = self.db.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Database is closed"))?;
        
        db.delete(key.to_vec())
            .map_err(|e| PyIOError::new_err(format!("Delete failed: {}", e)))
    }
    
    fn close(&mut self) -> PyResult<()> {
        if let Some(db) = self.db.take() {
            if let Ok(db_owned) = Arc::try_unwrap(db) {
                db_owned.close()
                    .map_err(|e| PyIOError::new_err(format!("Close failed: {}", e)))?;
            }
        }
        Ok(())
    }
    
    fn stats(&self) -> PyResult<DatabaseStats> {
        let db = self.db.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Database is closed"))?;
        
        let stats = db.stats();
        Ok(DatabaseStats {
            memtable_size: stats.memtable_size,
            memtable_entries: stats.memtable_entries,
            num_sstables: stats.num_sstables,
            sequence_number: stats.sequence_number,
        })
    }
    
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    
    fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

#[pyclass]
#[derive(Clone)]
struct DatabaseStats {
    #[pyo3(get)]
    memtable_size: usize,
    #[pyo3(get)]
    memtable_entries: usize,
    #[pyo3(get)]
    num_sstables: usize,
    #[pyo3(get)]
    sequence_number: u64,
}

#[pymodule]
fn middb_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add_class::<DatabaseStats>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_database_creation() {
        let temp_dir = std::env::temp_dir().join("middb_test_py");
        let _ = std::fs::remove_dir_all(&temp_dir);
        
        let db = Database::new(temp_dir.to_string_lossy().to_string()).unwrap();
        assert!(db.db.is_some());
    }
}
