use crate::expr::Expr;
use crate::plan::{LogicalPlan, PhysicalPlan};

pub struct Planner;

impl Planner {
    pub fn new() -> Self {
        Planner
    }
    
    pub fn plan(&self, scan_table: String, filter: Option<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: scan_table,
            filter,
        }
    }
    
    pub fn to_physical(&self, logical: LogicalPlan) -> PhysicalPlan {
        match logical {
            LogicalPlan::Scan { table, filter } => {
                PhysicalPlan::SeqScan { table, filter }
            }
            LogicalPlan::Filter { input, predicate } => {
                let child = self.to_physical(*input);
                PhysicalPlan::Filter {
                    input: Box::new(child),
                    predicate,
                }
            }
            LogicalPlan::Project { input, columns } => {
                let child = self.to_physical(*input);
                PhysicalPlan::Project {
                    input: Box::new(child),
                    columns,
                }
            }
        }
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}
