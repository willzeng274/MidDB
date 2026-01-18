import tempfile
import os
import shutil
import middb

def test_basic_operations():
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = middb.Database(temp_dir)
        
        db.put(b"key1", b"value1")
        
        value = db.get(b"key1")
        assert value == b"value1"
        
        value = db.get(b"nonexistent")
        assert value is None
        
        db.delete(b"key1")
        value = db.get(b"key1")
        assert value is None
        
        db.close()
    finally:
        shutil.rmtree(temp_dir)

def test_multiple_keys():
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = middb.Database(temp_dir)
        
        for i in range(100):
            key = f"key{i:03d}".encode()
            value = f"value{i}".encode()
            db.put(key, value)
        
        for i in range(100):
            key = f"key{i:03d}".encode()
            expected = f"value{i}".encode()
            actual = db.get(key)
            assert actual == expected
        
        db.close()
    finally:
        shutil.rmtree(temp_dir)

def test_context_manager():
    temp_dir = tempfile.mkdtemp()
    
    try:
        with middb.Database(temp_dir) as db:
            db.put(b"key", b"value")
            value = db.get(b"key")
            assert value == b"value"
    finally:
        shutil.rmtree(temp_dir)

def test_stats():
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = middb.Database(temp_dir)
        
        stats = db.stats()
        assert stats.memtable_entries == 0
        
        db.put(b"key", b"value")
        
        stats = db.stats()
        assert stats.memtable_entries == 1
        assert stats.memtable_size > 0
        
        db.close()
    finally:
        shutil.rmtree(temp_dir)

def test_update():
    temp_dir = tempfile.mkdtemp()
    
    try:
        db = middb.Database(temp_dir)
        
        db.put(b"key", b"value1")
        db.put(b"key", b"value2")
        
        value = db.get(b"key")
        assert value == b"value2"
        
        db.close()
    finally:
        shutil.rmtree(temp_dir)
