use crate::expr::{Expr, Value};
use crate::plan::PhysicalPlan;
use middb_core::Database;
use std::sync::Arc;

pub struct Executor {
    #[allow(dead_code)]
    db: Arc<Database>,
}

impl Executor {
    pub fn new(db: Arc<Database>) -> Self {
        Executor { db }
    }
    
    pub fn execute(&self, plan: PhysicalPlan) -> Result<Vec<Row>, String> {
        match plan {
            PhysicalPlan::SeqScan { table, filter } => {
                self.execute_scan(&table, filter)
            }
            PhysicalPlan::Filter { input, predicate } => {
                let rows = self.execute(*input)?;
                Ok(rows.into_iter()
                    .filter(|row| self.eval_predicate(&predicate, row))
                    .collect())
            }
        }
    }
    
    fn execute_scan(&self, _table: &str, _filter: Option<Expr>) -> Result<Vec<Row>, String> {
        Ok(Vec::new())
    }
    
    fn eval_predicate(&self, _expr: &Expr, _row: &Row) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub fields: Vec<Value>,
}

impl Row {
    pub fn new(fields: Vec<Value>) -> Self {
        Row { fields }
    }
}
