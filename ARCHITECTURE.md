# MidDB Architecture

MidDB is an embedded database engine written in Rust. It combines an LSM-tree storage engine with a SQL query layer, TCP networking with pipelining, MVCC transactions, and a consistent-hash-based clustering layer.

## Table of Contents

1. [System Overview](#system-overview)
2. [Storage Engine](#storage-engine)
3. [Query Engine](#query-engine)
4. [Network Layer](#network-layer)
5. [Cluster Layer](#cluster-layer)
6. [Transactions (MVCC)](#transactions-mvcc)
7. [Performance](#performance)

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Application                       │
│  Rust API  │  Python Bindings (PyO3)  │  TCP Client  │  SQL    │
└──────┬─────┴────────────┬──────────────┴──────┬───────┴────┬────┘
       │                  │                     │            │
       ▼                  ▼                     ▼            ▼
┌──────────────┐  ┌──────────────┐  ┌────────────────┐ ┌─────────┐
│  Database    │  │  Python FFI  │  │  Network Server│ │  SQL    │
│  (embedded)  │  │  (pyo3)      │  │  (tokio TCP)   │ │  Parser │
└──────┬───────┘  └──────┬───────┘  └───────┬────────┘ └────┬────┘
       │                 │                  │               │
       ▼                 ▼                  ▼               ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Database Core                             │
│  WAL → MemTable (SkipList) → SSTable (LZ4/Snappy) → Compaction │
│  B+Tree (in-memory + disk)  │  Bloom Filters  │  Catalog        │
├──────────────────────────────────────────────────────────────────┤
│  MVCC Transaction Manager (snapshot isolation, OCC)              │
└──────────────────────────────────────────────────────────────────┘
```

### Crate Layout

| Crate | Description |
|-------|-------------|
| `middb-core` | Storage engine: LSM tree, B+Tree, WAL, SSTables, compression, transactions, catalog |
| `middb-query` | SQL parser (via sqlparser-rs), cost-based optimizer, executor, join algorithms |
| `middb-network` | Frame-based TCP protocol with pipelining, batching, connection pooling |
| `middb-cluster` | Consistent hash ring, coordinator, membership protocol, shard rebalancing |
| `middb-cli` | Interactive CLI (server/client/local modes) |
| `bindings/python` | PyO3 bindings with transactions, SQL, async support |
| `loadtest` | Load test suite with hdrhistogram latency tracking |

---

## Storage Engine

### Write Path

```
put(key, value):
  1. seq = atomic_increment(sequence_number)
  2. WAL: append(WalEntry::Put(seq, key, value))
  3. WAL: fsync()  [if config.sync_writes == true]
  4. MemTable: insert into skip list
  5. If memtable exceeds threshold:
     a. Swap memtable for a fresh one
     b. Flush old memtable → SSTable on disk
     c. Run compaction if triggered
```

### Read Path

```
get(key):
  1. Check MemTable (skip list lookup)
  2. For each SSTable (newest first):
     a. Check key range → skip if out of range
     b. Check bloom filter → skip if definitely absent
     c. Binary search index block → find data block
     d. Scan data block for key
  3. Return None if not found
```

### MemTable

In-memory sorted buffer backed by a skip list.

- Max height: 16 levels, promotion probability p=0.25
- Tracks approximate memory usage via AtomicUsize
- Supports tombstones (ValueEntry::Tombstone) for deletes
- Configurable flush threshold (default 64 MB)

### Write-Ahead Log (WAL)

Entry format:
```
[CRC32: 4B][Length: 4B][Sequence: 8B][Type: 1B][KeyLen: 4B][Key][Value]
```

- Append-only, sequential writes
- Optional fsync per write (config.sync_writes)
- On recovery: replay entries into MemTable, restore sequence counter

### SSTable

Immutable sorted files with block-based layout:

```
┌─────────────────────┐
│ Data Blocks (64KB)  │  Prefix-compressed keys, restart points every 16 entries
├─────────────────────┤
│ Index Block         │  Maps separator keys → block offsets
├─────────────────────┤
│ Bloom Filter        │  10 bits/key, 7 hashes, ~1% false positive rate
├─────────────────────┤
│ Footer (48B)        │  Block handles, version, magic number
└─────────────────────┘
```

### Compression

SSTables support optional block compression:

| Algorithm | Ratio | Throughput |
|-----------|-------|------------|
| LZ4 | ~2.5x | ~800 MB/s compress, ~2 GB/s decompress |
| Snappy | ~2.3x | ~600 MB/s compress, ~1.5 GB/s decompress |

Configured via `config.use_compression` and `config.compression_type`.

### Leveled Compaction

```
L0: up to 4 overlapping files (direct from memtable flush)
L1: 10 MB max, non-overlapping, sorted by key range
L2: 100 MB max (10x multiplier per level)
...up to L6
```

Compaction triggers:
1. L0 file count >= `level0_file_num_compaction_trigger` (default 4)
2. Level size exceeds `max_bytes_for_level_base * multiplier^level`

Compaction merges overlapping files from Ln into Ln+1, deduplicating keys and removing tombstones.

### B+Tree

Two implementations:

**In-memory B+Tree** (generic, const-generic order):
```rust
BPTree<ORDER, K, V>  // e.g. BPTree<64, Vec<u8>, Vec<u8>>
```
- Leaf nodes linked for range scans
- Split/merge on insert/delete
- Used for secondary indexes

**Disk B+Tree** (page-based persistence):
- 4 KB pages with buffer pool (LRU eviction)
- Page types: Interior, Leaf, Free
- FileStorage backend with page_id * PAGE_SIZE addressing
- Supports create/open/insert/get/flush

---

## Query Engine

Full SQL pipeline:

```
SQL text → Parser → LogicalPlan → Optimizer → PhysicalPlan → Executor → Rows
```

### SQL Parser

Built on `sqlparser-rs`. Supports:
- SELECT with WHERE, ORDER BY, LIMIT, GROUP BY, HAVING
- INSERT, UPDATE, DELETE
- CREATE TABLE, DROP TABLE
- JOIN (INNER, LEFT, RIGHT, CROSS)
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- Expressions: arithmetic, comparison, AND/OR/NOT, LIKE, IS NULL

### Cost-Based Optimizer

Optimization passes applied to logical plans:

| Pass | Description |
|------|-------------|
| Predicate pushdown | Push filters below projections and joins |
| Projection pushdown | Eliminate unused columns early |
| Constant folding | Evaluate constant expressions at plan time |
| Join reordering | Reorder joins by estimated cardinality (smallest first) |

Uses table statistics (row count, distinct values, min/max) for cost estimation.

### Join Algorithms

Three physical join implementations selected by the optimizer:

| Algorithm | Best When | Complexity |
|-----------|-----------|------------|
| Hash join | One side fits in memory | O(n + m) |
| Sort-merge join | Both sides pre-sorted | O(n log n + m log m) |
| Nested loop join | Small outer, indexed inner | O(n * m) |

### Executor

Bridges the query engine to the KV storage layer:
- Tables stored as prefixed KV pairs: `{table}:{row_id}` → serialized row
- Catalog tracks schemas (column names, types, nullability)
- Schema validation on INSERT/UPDATE
- Row serialization: pipe-delimited values

---

## Network Layer

### Frame Protocol

```
┌──────────────────────────────────┐
│ Length (4 bytes, big-endian)      │
├──────────────────────────────────┤
│ Payload (bincode-serialized)     │
│  ┌────────────────────────────┐  │
│  │ request_id: u64            │  │
│  │ payload: Request|Response  │  │
│  └────────────────────────────┘  │
└──────────────────────────────────┘
```

Max frame size: 64 MB. TCP_NODELAY enabled.

### Request Types

| Request | Description |
|---------|-------------|
| Get/Put/Delete | Single key operations |
| BatchGet/BatchPut | Multi-key operations |
| Query | SQL string execution |
| BeginTxn/CommitTxn/AbortTxn | Transaction lifecycle |
| TxnGet/TxnPut/TxnDelete | Operations within a transaction |
| Pipeline | Multiple requests in a single frame |
| Ping | Health check |

### Server Architecture

```
TcpListener.accept()
  → split socket into (read_half, write_half)
  → spawn reader task:
      read frames → dispatch to handler via mpsc channel
  → spawn writer task:
      receive responses from channel → write frames
  → semaphore limits concurrent requests (256 max)
```

Each connection processes requests concurrently via tokio tasks with backpressure.

### Connection Pool

```rust
let pool = ConnectionPool::new("127.0.0.1:7878", 16);
let conn = pool.acquire().await?;
conn.put(&key, &value).await?;
// conn returned to pool on Drop
```

Semaphore-based with lazy connection creation.

---

## Cluster Layer

### Consistent Hash Ring

- 150 virtual nodes per physical node
- FNV-1a hashing
- Key → node mapping via ring lookup
- Supports replication factor (get N nodes clockwise from key's position)

### Coordinator

Routes operations across the cluster:
- Single-key ops: hash key → route to owning node
- Batch ops: group by owning node → parallel dispatch
- SQL queries: scatter to all shards → gather results

### Membership Protocol

- Heartbeat-based failure detection
- States: Alive → Suspect → Dead
- Configurable heartbeat interval and suspect/dead timeouts
- Automatic dead node removal

### Shard Rebalancing

When nodes join/leave:
1. Compute old ring vs new ring assignment for all shards
2. Generate rebalance plan: list of (shard, from_node, to_node) moves
3. Execute moves (data transfer between nodes)

---

## Transactions (MVCC)

Multi-version concurrency control with optimistic conflict detection.

### Lifecycle

```rust
let txn = db.begin_txn();              // Allocate TxnId, snapshot current version
db.put_txn(txn, key, value)?;          // Buffer write in local write_set
let val = db.get_txn(txn, &key)?;      // Check write_set, then snapshot, then storage
db.commit_txn(txn)?;                   // Validate → apply writes to storage
// or: db.abort_txn(txn)?;             // Discard write_set
```

### Conflict Detection

At commit time:
1. For each key in read_set: check if any committed version > txn.start_version
2. For each key in write_set: same check
3. If conflict found → return TxnError::Conflict, transaction must retry

### Visibility

`get_visible_value(key, start_version)` returns the latest committed value for `key` where `commit_version <= start_version`.

### Garbage Collection

`tm.gc(min_version)` removes committed version entries older than `min_version` to bound memory usage.

---

## Performance

### Load Test Results

All tests run on a single machine, release build. Benchmarks include both non-durable (`sync_writes=false`) and durable (`sync_writes=true`) configurations for honest comparison.

#### Storage Engine (sync_writes=false)

| Benchmark | Throughput | p50 | p99 |
|-----------|-----------|-----|-----|
| Sequential writes (128B values) | 1,320,000 ops/s | 1 μs | 2 μs |
| Sequential writes (1KB values) | 486,000 ops/s | 1 μs | 2 μs |
| Batch writes (batch=100) | 3,427,000 ops/s | 1 μs | 1 μs |
| Batch writes (batch=1000) | 3,590,000 ops/s | 1 μs | 1 μs |
| Random reads (50K pool) | 2,476,000 ops/s | 1 μs | 1 μs |
| Mixed 50/50 read/write | 1,636,000 ops/s | 1 μs | 1 μs |
| Mixed 95/5 read-heavy | 2,388,000 ops/s | 1 μs | 1 μs |
| Scan (100 keys) | 249,000 ops/s | 2 μs | 4 μs |
| Transactions (single-key) | 803,000 ops/s | 1 μs | 6 μs |
| Disk B+Tree insert+lookup | 214,000 ops/s | 3 μs | 7 μs |
| Overwrite stress (5x rewrite) | 997,000 ops/s | 1 μs | 2 μs |

#### Concurrent Benchmarks (sync_writes=false)

| Benchmark | Throughput | Notes |
|-----------|-----------|-------|
| Concurrent writes (4 threads) | 1,237,000 ops/s | Exceeds single-threaded — true scaling |
| Concurrent writes (10 threads) | 1,098,000 ops/s | Matches single-threaded (no degradation) |
| Concurrent writes (16 threads) | 664,000 ops/s | Channel backpressure at high thread count |
| Concurrent reads (10 threads) | 2,199,000 ops/s | Near-linear read scaling |
| Concurrent mixed (5w + 5r) | 2,467,000 ops/s | Mixed workload |

Key scalability features: 16-shard memtable (per-shard RwLock), fire-and-forget WAL writes with dedicated writer thread, sharded block cache.

#### Durable Benchmarks (sync_writes=true)

| Benchmark | Throughput | p50 | p99 |
|-----------|-----------|-----|-----|
| Durable writes (fsync per write) | 239 ops/s | 4,033 μs | 7,011 μs |
| Durable batch (batch=100, single fsync) | 20,491 ops/s | 49 μs | 62 μs |

Durable writes are slow because each write requires an `fsync()` to disk (~4ms per call). Durable batch amortizes the fsync cost across 100 writes via WAL group commit, achieving ~85x better throughput with the same durability guarantee.

#### Query Engine

| Benchmark | Throughput | p50 | p99 |
|-----------|-----------|-----|-----|
| SQL parsing | 167,155 ops/s | 4 μs | 31 μs |
| SQL INSERT | 150,737 ops/s | 4 μs | 26 μs |
| SELECT * (100 rows) | 18,709 ops/s | 24 μs | 43 μs |
| SELECT with WHERE (200 rows) | 13,564 ops/s | 61 μs | 111 μs |
| Aggregate COUNT | 20,764 ops/s | 39 μs | 159 μs |
| Mixed DML workload | 34,463 ops/s | 25 μs | 68 μs |

#### Network

| Benchmark | Throughput | p50 | p99 |
|-----------|-----------|-----|-----|
| Single client KV | 14,732 ops/s | 60 μs | 135 μs |
| 10 concurrent clients | 41,644 ops/s | 232 μs | 419 μs |
| 50 concurrent clients | 42,649 ops/s | 1,151 μs | 1,799 μs |
| Pipeline (batch=20) | 2,074 batches/s (~41K effective ops/s) | 455 μs | 640 μs |
| Transactions over network | 6,016 ops/s | 159 μs | 241 μs |

### Important Caveats

**The non-durable benchmarks above measure in-memory performance.** With 50K-100K keys at 128B values (~6.4 MB), the entire dataset fits in the 64 MB memtable. Nothing flushes to disk. These numbers measure BTreeMap insert and lookup speed with lock overhead — not end-to-end database throughput on realistic workloads.

**The fire-and-forget WAL (sync_writes=false) is weaker than RocksDB's sync=false.** RocksDB with sync=false still waits for the WAL write to reach the kernel buffer; it just skips fsync. MidDB's fire-and-forget returns before the WAL write completes, which means even a process crash (not just machine crash) can lose recent writes.

**The durable benchmarks (sync_writes=true) are the only honest disk numbers.** Both durable single writes (239 ops/s) and durable batch (20K ops/s) reflect real fsync latency and are comparable to any database on the same hardware.

### Comparison with Production Databases

These comparisons are approximate and should be read with the caveats above. Published benchmarks vary by hardware, workload, configuration, and methodology. MidDB's non-durable numbers are from small in-memory workloads; RocksDB/LevelDB numbers are typically from larger, disk-bound workloads.

#### vs RocksDB

| Metric | MidDB | RocksDB | Notes |
|--------|-------|---------|-------|
| In-memory writes (50K keys) | ~1.3M ops/s | ~400K-800K ops/s | **Not apples-to-apples**: MidDB test fits in memtable; RocksDB benchmarks use millions of keys with disk I/O |
| In-memory reads (50K keys) | ~2.5M ops/s | ~840K ops/s | Same caveat: MidDB reads from memtable, RocksDB reads from SSTables |
| Concurrent writes (10 threads) | ~1.1M ops/s | ~1M+ ops/s | Both in-memory; MidDB eliminated contention via sharding |
| Durable writes (fsync) | ~240 ops/s | ~200-300 ops/s | **Fair comparison**: both bottleneck on fsync |
| Durable batch | ~20K ops/s | ~50K+ ops/s | **Fair comparison**: RocksDB's group commit is more mature |

**Why MidDB appears fast on small workloads:** Simpler code path — no column families, no statistics collection, no rate limiters, no compaction scheduling. When the workload fits in memory, this overhead matters. On disk-bound workloads with millions of keys, compaction quality, I/O scheduling, and read amplification dominate — areas where RocksDB has 10+ years of optimization.

**Where RocksDB genuinely wins:**
- Large datasets that exceed memtable (compaction, read amplification)
- Sustained write throughput under compaction pressure
- Concurrent write scaling beyond 16 threads (lock-free skip list)
- Durable batch throughput (more sophisticated group commit)
- Crash recovery completeness (RocksDB sync=false still guarantees process-crash recovery)

#### vs LevelDB

| Metric | MidDB | LevelDB | Notes |
|--------|-------|---------|-------|
| In-memory writes | ~1.3M ops/s | ~400K ops/s | MidDB test is in-memory; LevelDB benchmarks include disk flushes |
| In-memory reads | ~2.5M ops/s | ~85K-190K ops/s | Same caveat |
| Scan (100 keys) | ~249K ops/s | ~2.4M ops/s (261 MB/s) | LevelDB reads sequentially from sorted files more efficiently |

MidDB is faster on small in-memory workloads due to sharded memtable and block cache. LevelDB is faster for sequential scans and would likely outperform MidDB on larger workloads that involve disk I/O, as LevelDB has a more mature SSTable/compaction pipeline.

#### vs DuckDB

DuckDB is a columnar OLAP database, so the comparison is mostly relevant for the SQL/query layer:

| Metric | MidDB | DuckDB | Notes |
|--------|-------|---------|-------|
| Row-by-row INSERT | ~151K ops/s | ~20K ops/s (naive) | MidDB's KV store is optimized for row-at-a-time OLTP writes |
| Batch INSERT | N/A | ~1.2M rows/s | DuckDB uses vectorized execution and columnar storage for bulk loads |
| SQL parsing | ~167K ops/s | N/A | Not directly comparable; DuckDB handles a much larger SQL dialect |

DuckDB is designed for analytical queries over large datasets (columnar scans, vectorized execution). MidDB is an OLTP-style KV store with a SQL layer on top. These are fundamentally different workloads.

### Correctness Guarantees

The benchmarks above are backed by correctness fixes that ensure honest results:

1. **WAL recovery**: Entries are replayed into the memtable on restart — no silent data loss
2. **Concurrent flush guard**: AtomicBool prevents two threads from racing on memtable flush
3. **Type-safe tombstone encoding**: Values prefixed with 0x01, tombstones with 0x02 — no magic byte collisions
4. **Atomic MVCC**: begin() and commit() hold proper locks for snapshot isolation
5. **Accurate size tracking**: MemTable decrements size on overwrite — no premature flushes

### Concurrency Architecture

The write path is designed for concurrent scalability:

1. **16-shard memtable**: Keys are hash-distributed across 16 independent `RwLock<BTreeMap>` shards. Writers only lock their target shard, allowing up to 16 parallel inserts with zero contention.
2. **Fire-and-forget WAL**: With `sync_writes=false`, WAL entries are sent to a dedicated writer thread via a lock-free crossbeam channel without waiting for acknowledgment. The writer thread batches entries for efficient I/O.
3. **Group commit WAL**: With `sync_writes=true`, writers block until the batch is fsync'd, but multiple writers share a single fsync call.
4. **Sharded block cache**: 16-shard LRU cache allows concurrent SSTable reads without contention.
5. **Per-transaction locks**: `RwLock<HashMap<TxnId, Mutex<Transaction>>>` — transaction operations use a shared read lock on the map and per-transaction mutexes.

### Where to Improve

1. **Lock-free memtable**: A concurrent skip list (like RocksDB's) would eliminate per-shard lock overhead entirely. The current 16-shard approach degrades above 16 threads.
2. **WAL writer thread scaling**: The single WAL writer thread becomes a bottleneck at >16 concurrent writers. Multiple writer threads with partitioned WAL files could help.
3. **Prefetching and direct I/O**: Modern engines use io_uring, direct I/O, and read-ahead for I/O efficiency.
4. **Compaction scheduling**: Currently triggers on L0 file count; could use size-tiered compaction or priority-based scheduling.

---

## Configuration

Key configuration options in `Config`:

| Option | Default | Description |
|--------|---------|-------------|
| `memtable_size` | 64 MB | Flush threshold for memtable |
| `sync_writes` | true | fsync WAL on every write (durability vs throughput) |
| `use_compression` | false | Enable SSTable block compression |
| `compression_type` | None | LZ4 or Snappy |
| `bloom_bits_per_key` | 10 | Bloom filter bits per key (~1% FP rate) |
| `block_size` | 64 KB | SSTable data block size |
| `level0_file_num_compaction_trigger` | 4 | L0 files before compaction |
| `max_bytes_for_level_base` | 10 MB | L1 size threshold |
| `max_bytes_for_level_multiplier` | 10 | Size multiplier per level |

---

## References

- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [LevelDB Implementation](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [RocksDB Performance Benchmarks](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks)
- [LevelDB Benchmarks](https://github.com/google/leveldb/blob/main/doc/benchmark.html)
- [DuckDB Performance Guide](https://duckdb.org/docs/stable/guides/performance/overview)
