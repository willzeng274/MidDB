use crate::expr::{BinaryOperator, Expr, Value};
use crate::plan::PhysicalPlan;
use std::cmp::Ordering;
use std::collections::HashMap;

pub struct Executor {
    tables: HashMap<String, Table>,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            tables: HashMap::new(),
        }
    }
    
    pub fn register_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }
    
    pub fn execute(&self, plan: PhysicalPlan) -> Result<Vec<Row>, String> {
        match plan {
            PhysicalPlan::SeqScan { table, filter } => {
                self.execute_scan(&table, filter)
            }
            PhysicalPlan::Filter { input, predicate } => {
                let rows = self.execute(*input)?;
                Ok(rows.into_iter()
                    .filter(|row| self.eval_expr(&predicate, row).map(|v| v.as_bool().unwrap_or(false)).unwrap_or(false))
                    .collect())
            }
            PhysicalPlan::Project { input, columns } => {
                let rows = self.execute(*input)?;
                Ok(rows.into_iter()
                    .map(|row| self.project_row(row, &columns))
                    .collect())
            }
        }
    }
    
    fn execute_scan(&self, table_name: &str, filter: Option<Expr>) -> Result<Vec<Row>, String> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?;
        
        let mut rows = table.rows.clone();
        
        if let Some(predicate) = filter {
            rows.retain(|row| {
                self.eval_expr(&predicate, row)
                    .map(|v| v.as_bool().unwrap_or(false))
                    .unwrap_or(false)
            });
        }
        
        Ok(rows)
    }
    
    fn eval_expr(&self, expr: &Expr, row: &Row) -> Option<Value> {
        match expr {
            Expr::Literal(value) => Some(value.clone()),
            Expr::Column(name) => row.get_column(name),
            Expr::BinaryOp { op, left, right } => {
                let left_val = self.eval_expr(left, row)?;
                let right_val = self.eval_expr(right, row)?;
                self.eval_binary_op(*op, left_val, right_val)
            }
        }
    }
    
    fn eval_binary_op(&self, op: BinaryOperator, left: Value, right: Value) -> Option<Value> {
        match op {
            BinaryOperator::Eq => Some(Value::Bool(left == right)),
            BinaryOperator::Ne => Some(Value::Bool(left != right)),
            BinaryOperator::Lt => left.compare(&right).map(|ord| Value::Bool(ord == Ordering::Less)),
            BinaryOperator::Le => left.compare(&right).map(|ord| Value::Bool(ord != Ordering::Greater)),
            BinaryOperator::Gt => left.compare(&right).map(|ord| Value::Bool(ord == Ordering::Greater)),
            BinaryOperator::Ge => left.compare(&right).map(|ord| Value::Bool(ord != Ordering::Less)),
            BinaryOperator::And => {
                match (left.as_bool(), right.as_bool()) {
                    (Some(a), Some(b)) => Some(Value::Bool(a && b)),
                    _ => None,
                }
            }
            BinaryOperator::Or => {
                match (left.as_bool(), right.as_bool()) {
                    (Some(a), Some(b)) => Some(Value::Bool(a || b)),
                    _ => None,
                }
            }
        }
    }
    
    fn project_row(&self, row: Row, columns: &[String]) -> Row {
        let mut fields = Vec::new();
        for col in columns {
            if let Some(value) = row.get_column(col) {
                fields.push(value);
            }
        }
        Row::new(fields)
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    columns: HashMap<String, Value>,
}

impl Row {
    pub fn new_with_values(columns: Vec<(String, Value)>) -> Self {
        Row {
            columns: columns.into_iter().collect(),
        }
    }
    
    pub fn new(fields: Vec<Value>) -> Self {
        let columns = fields.into_iter()
            .enumerate()
            .map(|(i, v)| (format!("col{}", i), v))
            .collect();
        Row { columns }
    }
    
    pub fn get_column(&self, name: &str) -> Option<Value> {
        self.columns.get(name).cloned()
    }
    
    pub fn fields(&self) -> Vec<Value> {
        self.columns.values().cloned().collect()
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Table {
            name,
            rows: Vec::new(),
        }
    }
    
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }
}
