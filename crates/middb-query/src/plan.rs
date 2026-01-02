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
}
