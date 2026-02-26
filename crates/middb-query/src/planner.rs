use crate::expr::Expr;
use crate::optimizer::CostBasedOptimizer;
use crate::parser::SqlParser;
use crate::plan::{LogicalPlan, PhysicalPlan};

pub struct Planner {
    optimizer: CostBasedOptimizer,
}

impl Planner {
    pub fn new() -> Self {
        Planner {
            optimizer: CostBasedOptimizer::new(),
        }
    }

    pub fn with_optimizer(optimizer: CostBasedOptimizer) -> Self {
        Planner { optimizer }
    }

    pub fn plan(&self, scan_table: String, filter: Option<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: scan_table,
            filter,
        }
    }

    pub fn plan_sql(&self, sql: &str) -> Result<PhysicalPlan, String> {
        let logical = SqlParser::parse(sql)?;
        Ok(self.optimizer.optimize(logical))
    }

    pub fn to_physical(&self, logical: LogicalPlan) -> PhysicalPlan {
        self.optimizer.optimize(logical)
    }
}

impl Default for Planner {
    fn default() -> Self {
        Self::new()
    }
}
