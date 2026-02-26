# MidDB

Embedded key-value database in Rust with LSM tree storage and B+Tree indexing.

Educational implementation demonstrating database internals. Not production-ready.

## Features

- LSM tree with MemTable, WAL, and SSTables
- Leveled compaction (L0, L1, L2, etc) with background worker
- MVCC transactions with snapshot isolation and conflict detection
- Catalog system for table schemas and type validation
- B+Tree in-memory indexes with node splitting
- Bloom filters for read optimization
- Block-based SSTable format with prefix compression
- Network protocol for client-server communication
- Query planner with logical/physical plans and catalog integration
- Python bindings via PyO3
- CLI with local REPL and remote client modes

## Quick Start

```bash
cargo build --release

# Run local database REPL
cargo run --bin middb-cli -- local --data-dir ./mydb

# Or start server
cargo run --bin middb-cli -- server --bind 127.0.0.1:7878 --data-dir ./data

# Connect client (in another terminal)
cargo run --bin middb-cli -- client --server 127.0.0.1:7878
```

## Usage

### Embedded Library

```rust
use middb_core::{Config, Database};

let config = Config::new("./data");
let db = Database::open(config)?;

// Basic operations
db.put(b"user:1".to_vec(), b"{\"name\":\"Alice\"}".to_vec())?;
let value = db.get(&b"user:1".to_vec())?;
db.delete(b"user:1".to_vec())?;

// Transactions
let txn = db.begin_txn();
db.put_txn(txn, b"key1".to_vec(), b"value1".to_vec())?;
db.put_txn(txn, b"key2".to_vec(), b"value2".to_vec())?;
db.commit_txn(txn)?;  // atomic commit

db.close()?;
```

### Python

```python
import middb

db = middb.Database("./data")
db.put(b"key", b"value")
value = db.get(b"key")  # => b"value"
db.close()
```

### CLI Commands

Local mode:
```
middb> put key1 hello
OK
middb> get key1
hello
middb> delete key1
OK
middb> stats
MemTable size: 256 bytes
MemTable entries: 0
SSTables: 0
Sequence: 2
```

Client mode (same commands work over network).

## Architecture

```
Write Path:  Client → WAL (fsync) → MemTable → Flush → L0 → Compaction → L1+
Read Path:   Client → MemTable → L0 (all files) → L1+ (binary search)
Txn Path:    begin → buffer writes → commit (conflict check) → apply
```

Components:
- MemTable: BTreeMap write buffer with tombstone support and accurate size tracking
- SSTable: Immutable sorted files with LZ4/Snappy block compression
- VersionSet: Level-organized SSTable management (L0 overlapping, L1+ sorted)
- Compaction: Background L0→L1 merging when L0 has 4+ files
- WAL: Append-only durability log with CRC checksums, group commit for concurrent writers
- Block Cache: 16-shard LRU cache for SSTable blocks
- Transactions: MVCC with snapshot isolation, atomic begin/commit, conflict detection
- Catalog: Table schemas with column types (Int64, String, Bytes, Bool)
- Bloom: 10 bits/key, ~1% false positive rate
- B+Tree: In-memory + disk-persistent with buffer pool
- Storage: Page abstraction (4KB pages)
- Network: Async TCP server/client with bincode protocol, connection pooling
- Query: SQL parser, cost-based optimizer, hash/sort-merge/nested loop joins

## Project Structure

```
middb/
├── crates/
│   ├── middb-core/        # Storage engine, compaction, transactions, catalog
│   ├── middb-network/     # TCP protocol
│   ├── middb-query/       # Query planner with catalog integration
│   └── middb-cli/         # Command-line tool
└── bindings/
    └── python/            # Python bindings (PyO3 + maturin)
```

## Examples

```bash
cargo run --example database_demo          # Basic put/get/delete
cargo run --example transaction_demo       # MVCC transactions
cargo run --example lsm_demo              # MemTable → SSTable flush
cargo run --example btree_comparison      # B+Tree vs SkipList
cargo run --example network_demo          # Client-server protocol
cargo run --example query_demo            # Query planning
cargo run --example performance_comparison --release
```

## Testing

```bash
cargo test                    # Run all tests
cargo test --package middb-core
cargo bench                   # Run benchmarks
```

## Implementation Status

Completed:
- LSM tree: MemTable, SSTable, WAL, bloom filters
- Leveled compaction: VersionSet, L0→L1 merging, background worker
- MVCC transactions: Snapshot isolation, conflict detection, atomic begin/commit
- Catalog: Table schemas, column types, query validation
- B+Tree: In-memory with node splitting + disk-persistent B+Tree with buffer pool
- Storage: File and memory backends
- Network: TCP server/client with bincode protocol, connection pooling
- Query: SQL parser, cost-based optimizer, join algorithms (hash/sort-merge/nested loop), executor
- CLI: Server, client, local REPL modes
- Python bindings: PyO3 with maturin build
- Compression: LZ4 and Snappy for SSTable blocks
- WAL group commit for concurrent write batching
- Sharded LRU block cache (16 shards)
- Cluster: Consistent hash ring, coordinator, membership protocol, shard rebalancing

## Development

```bash
cargo clippy                  # Lint
cargo fmt                     # Format
cargo doc --open              # Generate docs
```

## License

MIT OR Apache-2.0

## References

Design inspired by LevelDB, RocksDB, and DuckDB.
