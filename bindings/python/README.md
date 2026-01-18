# MidDB Python Bindings

Python bindings for MidDB using PyO3.

## Installation

```bash
cd bindings/python
uv venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
uv add maturin pytest
maturin develop --release
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
pytest tests/
python example.py
```

All 5 Python tests pass.

## Async API

Python wrapper for async operations using ThreadPoolExecutor:

```python
from concurrent.futures import ThreadPoolExecutor
import asyncio
import middb

class AsyncDatabase:
    def __init__(self, path):
        self.db = middb.Database(path)
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    async def put(self, key: bytes, value: bytes):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self.db.put, key, value)
    
    async def get(self, key: bytes):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self.db.get, key)

async def main():
    async with AsyncDatabase('./data') as db:
        await db.put(b'key', b'value')
        value = await db.get(b'key')

asyncio.run(main())
```

Async wrapper allows concurrent operations:
```python
results = await asyncio.gather(
    db.get(b"key1"),
    db.get(b"key2"),
    db.get(b"key3"),
)
```

See `example_async.py` for full implementation.

## API

`Database(path: str)` - Open or create database at path

Methods:
- `put(key: bytes, value: bytes) -> None`
- `get(key: bytes) -> Optional[bytes]`
- `delete(key: bytes) -> None`
- `stats() -> DatabaseStats`
- `close() -> None`

`DatabaseStats` properties:
- `memtable_size: int`
- `memtable_entries: int`
- `num_sstables: int`
- `sequence_number: int`
