use middb_query::{BinaryOperator, Executor, Expr, Planner, Row, Table, Value};

fn main() {
    println!("Query Executor Demo\n");
    
    let mut executor = Executor::new();
    
    let mut users = Table::new("users".to_string());
    users.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(1)),
        ("name".to_string(), Value::String("Alice".to_string())),
        ("age".to_string(), Value::Int(30)),
    ]));
    users.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(2)),
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Int(25)),
    ]));
    users.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(3)),
        ("name".to_string(), Value::String("Charlie".to_string())),
        ("age".to_string(), Value::Int(35)),
    ]));
    
    executor.register_table("users".to_string(), users);
    
    println!("Table 'users' with 3 rows registered\n");
    
    println!("Query 1: SELECT * FROM users");
    let planner = Planner::new();
    let logical = planner.plan("users".to_string(), None);
    let physical = planner.to_physical(logical);
    
    match executor.execute(physical) {
        Ok(rows) => {
            println!("Result: {} rows", rows.len());
            for row in &rows {
                println!("  {:?}", row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\nQuery 2: SELECT * FROM users WHERE age > 25");
    let filter = Expr::BinaryOp {
        op: BinaryOperator::Gt,
        left: Box::new(Expr::Column("age".to_string())),
        right: Box::new(Expr::Literal(Value::Int(25))),
    };
    
    let logical = planner.plan("users".to_string(), Some(filter));
    let physical = planner.to_physical(logical);
    
    match executor.execute(physical) {
        Ok(rows) => {
            println!("Result: {} rows", rows.len());
            for row in &rows {
                if let Some(name) = row.get_column("name") {
                    if let Some(age) = row.get_column("age") {
                        println!("  name={:?}, age={:?}", name, age);
                    }
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\nQuery 3: SELECT * FROM users WHERE age >= 30 AND age < 35");
    let complex_filter = Expr::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Ge,
            left: Box::new(Expr::Column("age".to_string())),
            right: Box::new(Expr::Literal(Value::Int(30))),
        }),
        right: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Lt,
            left: Box::new(Expr::Column("age".to_string())),
            right: Box::new(Expr::Literal(Value::Int(35))),
        }),
    };
    
    let logical = planner.plan("users".to_string(), Some(complex_filter));
    let physical = planner.to_physical(logical);
    
    match executor.execute(physical) {
        Ok(rows) => {
            println!("Result: {} rows", rows.len());
            for row in &rows {
                if let Some(name) = row.get_column("name") {
                    if let Some(age) = row.get_column("age") {
                        println!("  name={:?}, age={:?}", name, age);
                    }
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\nQuery execution complete");
}
