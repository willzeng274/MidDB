use crate::expr::Expr;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        filter: Option<Expr>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        filter: Option<Expr>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<String>,
    },
}
