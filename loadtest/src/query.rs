use crate::report::LoadTestRunner;
use middb_core::{Config, Database};
use middb_query::{Executor, Planner, SqlParser};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

pub fn run_all() {
    sql_parse_throughput(10_000);
    insert_throughput(500);
    select_throughput(100);
    filtered_select(100);
    aggregate_queries(100);
    mixed_dml_workload(200);
}

fn make_db() -> (Arc<Database>, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = Config::new(dir.path());
    config.sync_writes = false;
    (Arc::new(Database::open(config).unwrap()), dir)
}

fn exec(db: &Arc<Database>, sql: &str) -> Result<Vec<middb_query::Row>, String> {
    let logical = SqlParser::parse(sql)?;
    let planner = Planner::new();
    let physical = planner.to_physical(logical);
    let executor = Executor::with_database(Arc::clone(db));
    executor.execute(physical)
}

fn sql_parse_throughput(count: u64) {
    let mut runner = LoadTestRunner::new(&format!("sql_parse (n={})", count));
    let queries = vec![
        "SELECT * FROM users WHERE id = 1",
        "INSERT INTO orders VALUES (1, 'widget', 100)",
        "SELECT name, COUNT(*) FROM users GROUP BY name",
        "UPDATE products SET price = 50 WHERE id = 3",
        "SELECT a.id, b.name FROM a JOIN b ON a.id = b.a_id WHERE a.active = true",
        "DELETE FROM logs WHERE created_at < 1000",
        "SELECT * FROM items ORDER BY price DESC LIMIT 10",
        "CREATE TABLE IF NOT EXISTS temp (id INT, val TEXT)",
    ];

    runner.start();
    for i in 0..count {
        let sql = queries[(i as usize) % queries.len()];
        let op_start = Instant::now();
        match SqlParser::parse(sql) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn insert_throughput(count: u64) {
    let (db, _dir) = make_db();
    exec(&db, "CREATE TABLE bench_insert (id INT, name TEXT, score INT)").unwrap();

    let mut runner = LoadTestRunner::new(&format!("sql_insert (n={})", count));

    runner.start();
    for i in 0..count {
        let sql = format!("INSERT INTO bench_insert VALUES ({}, 'user_{}', {})", i, i, i * 10);
        let op_start = Instant::now();
        match exec(&db, &sql) {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn select_throughput(count: u64) {
    let (db, _dir) = make_db();
    exec(&db, "CREATE TABLE bench_select (id INT, name TEXT, val INT)").unwrap();
    for i in 0..100 {
        exec(&db, &format!("INSERT INTO bench_select VALUES ({}, 'item_{}', {})", i, i, i * 3)).unwrap();
    }

    let mut runner = LoadTestRunner::new(&format!("sql_select_all (n={}, rows=100)", count));

    runner.start();
    for _ in 0..count {
        let op_start = Instant::now();
        match exec(&db, "SELECT * FROM bench_select") {
            Ok(rows) => {
                assert!(!rows.is_empty());
                runner.record_op(op_start.elapsed());
            }
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn filtered_select(count: u64) {
    let (db, _dir) = make_db();
    exec(&db, "CREATE TABLE bench_filter (id INT, category TEXT, price INT)").unwrap();
    for i in 0..200 {
        let cat = if i % 3 == 0 { "electronics" } else if i % 3 == 1 { "books" } else { "food" };
        exec(&db, &format!("INSERT INTO bench_filter VALUES ({}, '{}', {})", i, cat, i * 5)).unwrap();
    }

    let mut runner = LoadTestRunner::new(&format!("sql_filtered_select (n={}, rows=200)", count));

    runner.start();
    for _ in 0..count {
        let op_start = Instant::now();
        match exec(&db, "SELECT * FROM bench_filter WHERE price > 500") {
            Ok(rows) => {
                assert!(!rows.is_empty());
                runner.record_op(op_start.elapsed());
            }
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn aggregate_queries(count: u64) {
    let (db, _dir) = make_db();
    exec(&db, "CREATE TABLE bench_agg (id INT, dept TEXT, salary INT)").unwrap();
    for i in 0..100 {
        let dept = match i % 4 {
            0 => "eng", 1 => "sales", 2 => "ops", _ => "hr",
        };
        exec(&db, &format!("INSERT INTO bench_agg VALUES ({}, '{}', {})", i, dept, 50000 + (i * 100))).unwrap();
    }

    let mut runner = LoadTestRunner::new(&format!("sql_aggregate (n={}, rows=100)", count));

    runner.start();
    for _ in 0..count {
        let op_start = Instant::now();
        match exec(&db, "SELECT COUNT(*) FROM bench_agg") {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}

fn mixed_dml_workload(count: u64) {
    let (db, _dir) = make_db();
    exec(&db, "CREATE TABLE bench_mixed (id INT, status TEXT, count INT)").unwrap();
    for i in 0..50 {
        exec(&db, &format!("INSERT INTO bench_mixed VALUES ({}, 'active', {})", i, i)).unwrap();
    }

    let mut runner = LoadTestRunner::new(&format!("sql_mixed_dml (n={})", count));
    let mut next_id = 50u64;
    let mut rng = 0xCAFEBABEu64;

    runner.start();
    for _ in 0..count {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let op_start = Instant::now();

        let result = match rng % 4 {
            0 => {
                let sql = format!("INSERT INTO bench_mixed VALUES ({}, 'active', {})", next_id, next_id);
                next_id += 1;
                exec(&db, &sql)
            }
            1 => exec(&db, "SELECT * FROM bench_mixed WHERE count > 25"),
            2 => {
                let id = rng % next_id;
                exec(&db, &format!("UPDATE bench_mixed SET status = 'updated' WHERE id = {}", id))
            }
            _ => exec(&db, "SELECT COUNT(*) FROM bench_mixed"),
        };

        match result {
            Ok(_) => runner.record_op(op_start.elapsed()),
            Err(_) => runner.record_error(),
        }
    }
    runner.finish().print();
}
