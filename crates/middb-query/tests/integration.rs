use middb_core::{Config, Database};
use middb_query::{Executor, Planner, SqlParser};
use std::sync::Arc;
use tempfile::TempDir;

fn setup_db() -> (Arc<Database>, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = Config::new(dir.path());
    let db = Arc::new(Database::open(config).unwrap());
    (db, dir)
}

fn execute_sql(db: &Arc<Database>, sql: &str) -> Result<Vec<middb_query::Row>, String> {
    let logical = SqlParser::parse(sql)?;
    let planner = Planner::new();
    let physical = planner.to_physical(logical);
    let executor = Executor::with_database(Arc::clone(db));
    executor.execute(physical)
}

#[test]
fn test_create_table_and_insert() {
    let (db, _dir) = setup_db();

    execute_sql(&db, "CREATE TABLE users (id INT, name TEXT)").unwrap();

    assert!(db.get_schema("users").is_some());

    let rows = execute_sql(&db, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").unwrap();
    assert_eq!(rows.len(), 1); // returns rows_affected
}

#[test]
fn test_select_after_insert() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE items (id INT, name TEXT, price INT)").unwrap();
    execute_sql(&db, "INSERT INTO items VALUES (1, 'widget', 100), (2, 'gadget', 200), (3, 'doohickey', 50)").unwrap();

    let rows = execute_sql(&db, "SELECT * FROM items").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_select_with_where() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE products (id INT, name TEXT, price INT)").unwrap();
    execute_sql(&db, "INSERT INTO products VALUES (1, 'cheap', 10), (2, 'mid', 50), (3, 'expensive', 200)").unwrap();

    let rows = execute_sql(&db, "SELECT * FROM products WHERE price > 20").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_drop_table() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE temp (id INT)").unwrap();
    assert!(db.get_schema("temp").is_some());

    execute_sql(&db, "DROP TABLE temp").unwrap();
    assert!(db.get_schema("temp").is_none());
}

#[test]
fn test_update_rows() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE scores (id INT, score INT)").unwrap();
    execute_sql(&db, "INSERT INTO scores VALUES (1, 50), (2, 75)").unwrap();

    execute_sql(&db, "UPDATE scores SET score = 100 WHERE id = 1").unwrap();

    let rows = execute_sql(&db, "SELECT * FROM scores WHERE id = 1").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_delete_rows() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE logs (id INT, msg TEXT)").unwrap();
    execute_sql(&db, "INSERT INTO logs VALUES (1, 'first'), (2, 'second'), (3, 'third')").unwrap();

    execute_sql(&db, "DELETE FROM logs WHERE id = 2").unwrap();

    let rows = execute_sql(&db, "SELECT * FROM logs").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_order_by_and_limit() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE nums (val INT)").unwrap();
    execute_sql(&db, "INSERT INTO nums VALUES (3), (1), (4), (1), (5)").unwrap();

    let rows = execute_sql(&db, "SELECT * FROM nums ORDER BY val LIMIT 3").unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
fn test_aggregate_count() {
    let (db, _dir) = setup_db();
    execute_sql(&db, "CREATE TABLE events (id INT, kind TEXT)").unwrap();
    execute_sql(&db, "INSERT INTO events VALUES (1, 'click'), (2, 'click'), (3, 'view'), (4, 'click')").unwrap();

    let rows = execute_sql(&db, "SELECT COUNT(*) FROM events").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_sql_parse_errors() {
    let (db, _dir) = setup_db();
    let result = execute_sql(&db, "SELECTT * FROM nothing");
    assert!(result.is_err());
}
