use crate::expr::{AggregateFunc, Expr};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Cross,
}

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
        columns: Vec<Expr>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr, String)>,
    },
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<(Expr, bool)>,
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: usize,
        offset: usize,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    CreateTable {
        table: String,
        columns: Vec<(String, String)>,
        if_not_exists: bool,
    },
    DropTable {
        table: String,
        if_exists: bool,
    },
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan {
        table: String,
        filter: Option<Expr>,
    },
    IndexScan {
        table: String,
        index_column: String,
        filter: Option<Expr>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<Expr>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_key: Expr,
        right_key: Expr,
    },
    SortMergeJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_key: Expr,
        right_key: Expr,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(AggregateFunc, Expr, String)>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        order_by: Vec<(Expr, bool)>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        offset: usize,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
    },
    CreateTable {
        table: String,
        columns: Vec<(String, String)>,
        if_not_exists: bool,
    },
    DropTable {
        table: String,
        if_exists: bool,
    },
}
