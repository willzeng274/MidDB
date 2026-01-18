# MidDB Python Bindings

Python bindings for MidDB using PyO3.

## Installation

```bash
pip install maturin
maturin develop
```

## Usage

```python
import middb

db = middb.Database("./data")

db.put(b"key", b"value")
value = db.get(b"key")
db.delete(b"key")

stats = db.stats()
print(f"Entries: {stats.memtable_entries}")

db.close()
```

Context manager:
```python
with middb.Database("./data") as db:
    db.put(b"key", b"value")
    value = db.get(b"key")
```

## Testing

```bash
pip install pytest
maturin develop
pytest tests/
```

## API

`Database(path: str)` - Open or create database at path

Methods:
- `put(key: bytes, value: bytes) -> None` - Store key-value pair
- `get(key: bytes) -> Optional[bytes]` - Retrieve value for key
- `delete(key: bytes) -> None` - Delete key
- `stats() -> DatabaseStats` - Get database statistics
- `close() -> None` - Close database

`DatabaseStats` properties:
- `memtable_size: int` - MemTable size in bytes
- `memtable_entries: int` - Number of MemTable entries
- `num_sstables: int` - Number of SSTables
- `sequence_number: int` - Current sequence number
