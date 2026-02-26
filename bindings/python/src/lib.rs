use middb_core::{
    catalog::{DataType, TableSchemaBuilder},
    Config, Database as CoreDatabase, TxnId,
};
use middb_query::{Executor, Planner, SqlParser};
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
        let db = self.get_db()?;
        db.put(key.to_vec(), value.to_vec())
            .map_err(|e| PyIOError::new_err(format!("Put failed: {}", e)))
    }

    fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let db = self.get_db()?;
        match db.get(&key.to_vec()) {
            Ok(Some(value)) => Ok(Some(PyBytes::new_bound(py, &value))),
            Ok(None) => Ok(None),
            Err(e) => Err(PyIOError::new_err(format!("Get failed: {}", e))),
        }
    }

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        let db = self.get_db()?;
        db.delete(key.to_vec())
            .map_err(|e| PyIOError::new_err(format!("Delete failed: {}", e)))
    }

    fn begin_transaction(&self) -> PyResult<Transaction> {
        let db = self.get_db()?;
        let txn_id = db.begin_txn();
        Ok(Transaction {
            db: Arc::clone(db),
            txn_id: Some(txn_id),
        })
    }

    fn execute_sql(&self, sql: &str) -> PyResult<QueryResult> {
        let db = self.get_db()?;

        let logical_plan = SqlParser::parse(sql)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL parse error: {}", e)))?;

        let planner = Planner::new();
        let physical_plan = planner.to_physical(logical_plan);

        let executor = Executor::with_database(Arc::clone(db));
        let rows = executor.execute(physical_plan)
            .map_err(|e| PyRuntimeError::new_err(format!("SQL execution error: {}", e)))?;

        let columns: Vec<String> = if let Some(first) = rows.first() {
            first.column_order().to_vec()
        } else {
            vec![]
        };

        let result_rows: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|row| {
                columns.iter().map(|col| {
                    row.get_column(col).map(|v| format!("{:?}", v))
                }).collect()
            })
            .collect();

        Ok(QueryResult { columns, rows: result_rows })
    }

    fn create_table(&self, name: String, columns: Vec<(String, String)>) -> PyResult<()> {
        let db = self.get_db()?;
        let mut builder = TableSchemaBuilder::new(&name);
        for (col_name, col_type) in columns {
            let dt = match col_type.to_lowercase().as_str() {
                "int" | "integer" | "int64" => DataType::Int64,
                "string" | "text" | "varchar" => DataType::String,
                "bytes" | "blob" => DataType::Bytes,
                "bool" | "boolean" => DataType::Bool,
                other => return Err(PyRuntimeError::new_err(format!("Unknown type: {}", other))),
            };
            builder = builder.column(&col_name, dt, true);
        }
        db.create_table(builder.build())
            .map_err(|e| PyRuntimeError::new_err(format!("Create table failed: {}", e)))
    }

    fn list_tables(&self) -> PyResult<Vec<String>> {
        let db = self.get_db()?;
        Ok(db.list_tables())
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
        let db = self.get_db()?;
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

impl Database {
    fn get_db(&self) -> PyResult<&Arc<CoreDatabase>> {
        self.db.as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Database is closed"))
    }
}

#[pyclass]
struct Transaction {
    db: Arc<CoreDatabase>,
    txn_id: Option<TxnId>,
}

#[pymethods]
impl Transaction {
    fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let txn_id = self.active_txn()?;
        match self.db.get_txn(txn_id, &key.to_vec()) {
            Ok(Some(value)) => Ok(Some(PyBytes::new_bound(py, &value))),
            Ok(None) => Ok(None),
            Err(e) => Err(PyIOError::new_err(format!("TxnGet failed: {}", e))),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> PyResult<()> {
        let txn_id = self.active_txn()?;
        self.db.put_txn(txn_id, key.to_vec(), value.to_vec())
            .map_err(|e| PyIOError::new_err(format!("TxnPut failed: {}", e)))
    }

    fn delete(&self, key: &[u8]) -> PyResult<()> {
        let txn_id = self.active_txn()?;
        self.db.delete_txn(txn_id, key.to_vec())
            .map_err(|e| PyIOError::new_err(format!("TxnDelete failed: {}", e)))
    }

    fn commit(&mut self) -> PyResult<()> {
        let txn_id = self.take_txn()?;
        self.db.commit_txn(txn_id)
            .map_err(|e| PyRuntimeError::new_err(format!("Commit failed: {}", e)))
    }

    fn abort(&mut self) -> PyResult<()> {
        let txn_id = self.take_txn()?;
        self.db.abort_txn(txn_id)
            .map_err(|e| PyRuntimeError::new_err(format!("Abort failed: {}", e)))
    }
}

impl Transaction {
    fn active_txn(&self) -> PyResult<TxnId> {
        self.txn_id
            .ok_or_else(|| PyRuntimeError::new_err("Transaction already committed or aborted"))
    }

    fn take_txn(&mut self) -> PyResult<TxnId> {
        self.txn_id.take()
            .ok_or_else(|| PyRuntimeError::new_err("Transaction already committed or aborted"))
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(txn_id) = self.txn_id.take() {
            let _ = self.db.abort_txn(txn_id);
        }
    }
}

#[pyclass]
#[derive(Clone)]
struct QueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    rows: Vec<Vec<Option<String>>>,
}

#[pymethods]
impl QueryResult {
    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __repr__(&self) -> String {
        format!("QueryResult(columns={:?}, rows={})", self.columns, self.rows.len())
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
    m.add_class::<Transaction>()?;
    m.add_class::<QueryResult>()?;
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
