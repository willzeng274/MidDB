#!/usr/bin/env python3
import asyncio
from concurrent.futures import ThreadPoolExecutor
import middb
import tempfile
import shutil

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
    
    async def delete(self, key: bytes):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self.executor, self.db.delete, key)
    
    def stats(self):
        return self.db.stats()
    
    def close(self):
        self.executor.shutdown(wait=True)
        self.db.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

async def main():
    temp_dir = tempfile.mkdtemp()
    
    print("Async Python Wrapper Demo\n")
    
    try:
        print("Opening database with async wrapper")
        db = AsyncDatabase(temp_dir)
        
        print("\nConcurrent writes:")
        await asyncio.gather(
            db.put(b"user:1", b'{"name": "Alice"}'),
            db.put(b"user:2", b'{"name": "Bob"}'),
            db.put(b"user:3", b'{"name": "Charlie"}'),
        )
        
        print("\nConcurrent reads:")
        results = await asyncio.gather(
            db.get(b"user:1"),
            db.get(b"user:2"),
            db.get(b"user:3"),
        )
        
        for i, value in enumerate(results, 1):
            if value:
                print(f"  user:{i} => {value.decode()}")
        
        print("\nStats:")
        stats = db.stats()
        print(f"  Entries: {stats.memtable_entries}")
        print(f"  Sequence: {stats.sequence_number}")
        
        print("\nAsync context manager:")
        async with AsyncDatabase(temp_dir + "_ctx") as db2:
            await db2.put(b"test", b"async_context")
            result = await db2.get(b"test")
            print(f"  test => {result.decode()}")
        
        db.close()
        print("\nDemo complete")
        
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
        shutil.rmtree(temp_dir + "_ctx", ignore_errors=True)

if __name__ == "__main__":
    asyncio.run(main())
