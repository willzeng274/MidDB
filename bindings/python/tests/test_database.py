import tempfile
import shutil
import middb


def test_basic_operations():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        db.put(b"key1", b"value1")
        assert db.get(b"key1") == b"value1"
        assert db.get(b"nonexistent") is None
        db.delete(b"key1")
        assert db.get(b"key1") is None
        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_multiple_keys():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        for i in range(100):
            db.put(f"key{i:03d}".encode(), f"value{i}".encode())
        for i in range(100):
            assert db.get(f"key{i:03d}".encode()) == f"value{i}".encode()
        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_context_manager():
    temp_dir = tempfile.mkdtemp()
    try:
        with middb.Database(temp_dir) as db:
            db.put(b"key", b"value")
            assert db.get(b"key") == b"value"
    finally:
        shutil.rmtree(temp_dir)


def test_stats():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        assert db.stats().memtable_entries == 0
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
        assert db.get(b"key") == b"value2"
        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_transaction_commit():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        txn = db.begin_transaction()
        txn.put(b"txn_key", b"txn_value")
        assert txn.get(b"txn_key") == b"txn_value"
        txn.commit()
        assert db.get(b"txn_key") == b"txn_value"
        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_transaction_abort():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        db.put(b"keep", b"original")
        txn = db.begin_transaction()
        txn.put(b"temp", b"will_abort")
        txn.abort()
        assert db.get(b"temp") is None
        assert db.get(b"keep") == b"original"
        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_list_tables():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        assert db.list_tables() == []
        db.create_table("users", [("id", "int"), ("name", "string")])
        tables = db.list_tables()
        assert "users" in tables
        db.close()
    finally:
        shutil.rmtree(temp_dir)


def test_execute_sql_create_and_insert():
    temp_dir = tempfile.mkdtemp()
    try:
        db = middb.Database(temp_dir)
        result = db.execute_sql("CREATE TABLE test (id INT, name TEXT)")
        assert len(result) == 0
        result = db.execute_sql("INSERT INTO test VALUES (1, 'alice'), (2, 'bob')")
        assert "test" in db.list_tables()
        db.close()
    finally:
        shutil.rmtree(temp_dir)
