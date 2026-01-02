use crate::expr::{BinaryOperator, Expr, Value};
use crate::plan::LogicalPlan;
use crate::planner::Planner;

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
