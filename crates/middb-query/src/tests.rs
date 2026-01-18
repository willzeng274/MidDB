use crate::expr::{BinaryOperator, Expr, Value};
use crate::plan::LogicalPlan;
use crate::planner::Planner;
use crate::{Executor, Row, Table};

#[test]
fn test_simple_scan_plan() {
    let planner = Planner::new();
    let plan = planner.plan("users".to_string(), None);
    
    match plan {
        LogicalPlan::Scan { table, filter } => {
            assert_eq!(table, "users");
            assert!(filter.is_none());
        }
        _ => panic!("Expected Scan plan"),
    }
}

#[test]
fn test_scan_with_filter() {
    let planner = Planner::new();
    let filter = Expr::BinaryOp {
        op: BinaryOperator::Eq,
        left: Box::new(Expr::Column("id".to_string())),
        right: Box::new(Expr::Literal(Value::Int(42))),
    };
    
    let plan = planner.plan("users".to_string(), Some(filter.clone()));
    
    match plan {
        LogicalPlan::Scan { table, filter: f } => {
            assert_eq!(table, "users");
            assert_eq!(f, Some(filter));
        }
        _ => panic!("Expected Scan plan"),
    }
}

#[test]
fn test_logical_to_physical() {
    let planner = Planner::new();
    let logical = LogicalPlan::Scan {
        table: "test".to_string(),
        filter: None,
    };
    
    let physical = planner.to_physical(logical);
    
    match physical {
        crate::plan::PhysicalPlan::SeqScan { table, .. } => {
            assert_eq!(table, "test");
        }
        _ => panic!("Expected SeqScan"),
    }
}

#[test]
fn test_executor_scan() {
    let mut executor = Executor::new();
    
    let mut table = Table::new("test".to_string());
    table.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(1)),
        ("name".to_string(), Value::String("Alice".to_string())),
    ]));
    table.add_row(Row::new_with_values(vec![
        ("id".to_string(), Value::Int(2)),
        ("name".to_string(), Value::String("Bob".to_string())),
    ]));
    
    executor.register_table("test".to_string(), table);
    
    let planner = Planner::new();
    let logical = planner.plan("test".to_string(), None);
    let physical = planner.to_physical(logical);
    
    let rows = executor.execute(physical).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_executor_filter() {
    let mut executor = Executor::new();
    
    let mut table = Table::new("test".to_string());
    table.add_row(Row::new_with_values(vec![
        ("age".to_string(), Value::Int(25)),
    ]));
    table.add_row(Row::new_with_values(vec![
        ("age".to_string(), Value::Int(30)),
    ]));
    table.add_row(Row::new_with_values(vec![
        ("age".to_string(), Value::Int(35)),
    ]));
    
    executor.register_table("test".to_string(), table);
    
    let filter = Expr::BinaryOp {
        op: BinaryOperator::Gt,
        left: Box::new(Expr::Column("age".to_string())),
        right: Box::new(Expr::Literal(Value::Int(27))),
    };
    
    let planner = Planner::new();
    let logical = planner.plan("test".to_string(), Some(filter));
    let physical = planner.to_physical(logical);
    
    let rows = executor.execute(physical).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_expression_evaluation() {
    let mut executor = Executor::new();
    
    let mut table = Table::new("test".to_string());
    table.add_row(Row::new_with_values(vec![
        ("a".to_string(), Value::Int(10)),
        ("b".to_string(), Value::Int(20)),
    ]));
    
    executor.register_table("test".to_string(), table);
    
    let filter = Expr::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Gt,
            left: Box::new(Expr::Column("a".to_string())),
            right: Box::new(Expr::Literal(Value::Int(5))),
        }),
        right: Box::new(Expr::BinaryOp {
            op: BinaryOperator::Lt,
            left: Box::new(Expr::Column("b".to_string())),
            right: Box::new(Expr::Literal(Value::Int(30))),
        }),
    };
    
    let planner = Planner::new();
    let logical = planner.plan("test".to_string(), Some(filter));
    let physical = planner.to_physical(logical);
    
    let rows = executor.execute(physical).unwrap();
    assert_eq!(rows.len(), 1);
}
