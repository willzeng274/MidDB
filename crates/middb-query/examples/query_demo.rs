use middb_query::{BinaryOperator, Executor, Expr, Planner, Row, Table, Value};

fn main() {
    println!("Query Engine Demo\n");
    
    let mut executor = Executor::new();
    
    let mut products = Table::new("products".to_string());
    products.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(1)),
        ("name".to_string(), Value::String("Laptop".to_string())),
        ("price".to_string(), Value::Int(1200)),
    ]));
    products.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(2)),
        ("name".to_string(), Value::String("Mouse".to_string())),
        ("price".to_string(), Value::Int(25)),
    ]));
    products.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(3)),
        ("name".to_string(), Value::String("Keyboard".to_string())),
        ("price".to_string(), Value::Int(75)),
    ]));
    products.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(4)),
        ("name".to_string(), Value::String("Monitor".to_string())),
        ("price".to_string(), Value::Int(300)),
    ]));
    
    executor.register_table("products".to_string(), products);
    
    println!("Registered 'products' table with 4 rows\n");
    
    let planner = Planner::new();
    
    println!("Query: SELECT * FROM products WHERE price > 100");
    let filter = Expr::BinaryOp {
        op: BinaryOperator::Gt,
        left: Box::new(Expr::Column("price".to_string())),
        right: Box::new(Expr::Literal(Value::Int(100))),
    };
    
    let logical = planner.plan("products".to_string(), Some(filter));
    let physical = planner.to_physical(logical);
    
    match executor.execute(physical) {
        Ok(rows) => {
            println!("Results: {} rows\n", rows.len());
            for row in &rows {
                if let (Some(name), Some(price)) = (row.get_column("name"), row.get_column("price")) {
                    println!("  {} - ${:?}", name.as_string().unwrap(), price.as_int().unwrap());
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\nQuery: SELECT * FROM products WHERE price >= 50 AND price <= 100");
    let range_filter = Expr::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Ge,
            left: Box::new(Expr::Column("price".to_string())),
            right: Box::new(Expr::Literal(Value::Int(50))),
        }),
        right: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Le,
            left: Box::new(Expr::Column("price".to_string())),
            right: Box::new(Expr::Literal(Value::Int(100))),
        }),
    };
    
    let logical = planner.plan("products".to_string(), Some(range_filter));
    let physical = planner.to_physical(logical);
    
    match executor.execute(physical) {
        Ok(rows) => {
            println!("Results: {} rows\n", rows.len());
            for row in &rows {
                if let (Some(name), Some(price)) = (row.get_column("name"), row.get_column("price")) {
                    println!("  {} - ${:?}", name.as_string().unwrap(), price.as_int().unwrap());
                }
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    println!("\nQuery engine with execution complete");
}
