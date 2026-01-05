# MidDB

Embedded key-value database in Rust with LSM tree storage and B+Tree indexing.

Educational implementation demonstrating database internals. Not production-ready.

## Features

- LSM tree write optimization with MemTable, WAL, and SSTables
- B+Tree in-memory indexes with node splitting
- Bloom filters for read optimization
- Block-based SSTable format with prefix compression
- Page-oriented storage abstraction
- Network protocol for client-server communication
- Query planner with logical and physical plans
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

db.put(b"user:1".to_vec(), b"{\"name\":\"Alice\"}".to_vec())?;
let value = db.get(&b"user:1".to_vec())?;
db.delete(b"user:1".to_vec())?;

db.close()?;
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
Write Path:  Client → WAL (fsync) → MemTable → Flush → SSTable (L0)
Read Path:   Client → MemTable → SSTables (with bloom filter checks)
Network:     TCP + bincode protocol
Query:       Expression AST → Logical Plan → Physical Plan
```

Components:
- MemTable: Skip list-backed write buffer
- SSTable: Sorted immutable files with block compression
- WAL: Append-only log for durability
- Bloom: Probabilistic filters (10 bits/key, ~1% FP rate)
- B+Tree: Balanced tree for in-memory indexing
- Storage: Page abstraction (4KB pages)
- Network: Async TCP server/client
- Query: Relational plan representation

## Project Structure

```
middb/
├── crates/
│   ├── middb-core/        # Storage engine
│   ├── middb-network/     # TCP protocol
│   ├── middb-query/       # Query planner
│   └── middb-cli/         # Command-line tool
└── bindings/
    └── python/            # Future Python bindings
```

## Examples

```bash
cargo run --example database_demo          # Full database lifecycle
cargo run --example lsm_demo              # LSM tree components
cargo run --example btree_comparison      # B+Tree vs SkipList
cargo run --example network_demo          # Client-server protocol
cargo run --example query_demo            # Query planning
cargo run --example performance_comparison --release
```

## Testing

```bash
cargo test                    # Run all tests (75 tests)
cargo test --package middb-core
cargo bench                   # Run benchmarks
```

## Implementation Status

Completed:
- LSM tree: MemTable, SSTable, WAL, bloom filters, flushing
- B+Tree: In-memory with node splitting
- Storage: File and memory backends
- Network: TCP server and client with bincode protocol
- Query: Expression AST, logical/physical plans, planner
- CLI: Server mode, client mode, local REPL

Deferred:
- Full leveled compaction (basic flushing works)
- B+Tree disk persistence (storage layer ready)
- Transactions and MVCC
- SQL parser
- Query optimizer
- Join algorithms

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
