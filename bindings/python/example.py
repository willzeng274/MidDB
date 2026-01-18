#!/usr/bin/env python3
import middb
import tempfile
import shutil

temp_dir = tempfile.mkdtemp()

print("Python Bindings Demo\n")
print(f"Using temp directory: {temp_dir}\n")

try:
    print("Opening database")
    db = middb.Database(temp_dir)
    
    print("\nInserting data")
    db.put(b"user:1", b'{"name": "Alice", "age": 30}')
    db.put(b"user:2", b'{"name": "Bob", "age": 25}')
    db.put(b"user:3", b'{"name": "Charlie", "age": 35}')
    
    print("\nRetrieving data:")
    for key in [b"user:1", b"user:2", b"user:3"]:
        value = db.get(key)
        if value:
            print(f"  {key.decode()} => {value.decode()}")
        else:
            print(f"  {key.decode()} => Not found")
    
    print("\nDatabase stats:")
    stats = db.stats()
    print(f"  MemTable size: {stats.memtable_size} bytes")
    print(f"  MemTable entries: {stats.memtable_entries}")
    print(f"  SSTables: {stats.num_sstables}")
    print(f"  Sequence: {stats.sequence_number}")
    
    print("\nDeleting user:2")
    db.delete(b"user:2")
    
    value = db.get(b"user:2")
    if value is None:
        print("  Deletion successful")
    else:
        print("  ERROR: Key still exists")
    
    print("\nUsing context manager:")
    with middb.Database(temp_dir + "_ctx") as db2:
        db2.put(b"test", b"context")
        print(f"  test => {db2.get(b'test').decode()}")
    
    print("\nClosing database")
    db.close()
    
    print("\nDemo complete")
    
finally:
    shutil.rmtree(temp_dir, ignore_errors=True)
    shutil.rmtree(temp_dir + "_ctx", ignore_errors=True)
