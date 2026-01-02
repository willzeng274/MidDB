use middb_query::{BinaryOperator, Expr, LogicalPlan, Planner, Value};

fn main() {
    println!("Query Engine Demo\n");
    
    let planner = Planner::new();
    
    println!("Creating simple scan plan:");
    let scan = planner.plan("users".to_string(), None);
    println!("{:?}\n", scan);
    
    println!("Creating scan with filter:");
    let filter = Expr::BinaryOp {
        op: BinaryOperator::Eq,
        left: Box::new(Expr::Column("age".to_string())),
        right: Box::new(Expr::Literal(Value::Int(25))),
    };
    
    let filtered_scan = planner.plan("users".to_string(), Some(filter.clone()));
    println!("{:?}\n", filtered_scan);
    
    println!("Converting to physical plan:");
    let physical = planner.to_physical(filtered_scan);
    println!("{:?}\n", physical);
    
    println!("Creating nested filter plan:");
    let nested = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            table: "products".to_string(),
            filter: None,
        }),
        predicate: Expr::BinaryOp {
            op: BinaryOperator::Gt,
            left: Box::new(Expr::Column("price".to_string())),
            right: Box::new(Expr::Literal(Value::Int(100))),
        },
    };
    
    println!("Logical plan:");
    println!("{:?}\n", nested);
    
    let physical_nested = planner.to_physical(nested);
    println!("Physical plan:");
    println!("{:?}\n", physical_nested);
    
    println!("Expression evaluation example:");
    let expr = Expr::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Gt,
            left: Box::new(Expr::Column("age".to_string())),
            right: Box::new(Expr::Literal(Value::Int(18))),
        }),
        right: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Lt,
            left: Box::new(Expr::Column("age".to_string())),
            right: Box::new(Expr::Literal(Value::Int(65))),
        }),
    };
    
    println!("{}", expr);
    println!("\nQuery engine components demonstrated");
}
