use crate::expr::{BinaryOperator, Expr, Value};
use crate::plan::PhysicalPlan;
use middb_core::catalog::{Catalog, DataType, TableSchema};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct Executor {
    tables: HashMap<String, Table>,
    catalog: Option<Arc<RwLock<Catalog>>>,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            tables: HashMap::new(),
            catalog: None,
        }
    }

    pub fn with_catalog(catalog: Arc<RwLock<Catalog>>) -> Self {
        Executor {
            tables: HashMap::new(),
            catalog: Some(catalog),
        }
    }

    pub fn set_catalog(&mut self, catalog: Arc<RwLock<Catalog>>) {
        self.catalog = Some(catalog);
    }

    pub fn register_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }

    pub fn validate_plan(&self, plan: &PhysicalPlan) -> Result<(), String> {
        let catalog = match &self.catalog {
            Some(c) => c.read().unwrap(),
            None => return Ok(()),
        };

        match plan {
            PhysicalPlan::SeqScan { table, filter } => {
                if !catalog.table_exists(table) && !self.tables.contains_key(table) {
                    return Err(format!("table not found: {}", table));
                }
                if let Some(expr) = filter {
                    if let Some(schema) = catalog.get_table(table) {
                        self.validate_expr(expr, schema)?;
                    }
                }
                Ok(())
            }
            PhysicalPlan::Filter { input, predicate } => {
                self.validate_plan(input)?;
                if let Some(table_name) = self.get_table_name(input) {
                    if let Some(schema) = catalog.get_table(&table_name) {
                        self.validate_expr(predicate, schema)?;
                    }
                }
                Ok(())
            }
            PhysicalPlan::Project { input, columns } => {
                self.validate_plan(input)?;
                if let Some(table_name) = self.get_table_name(input) {
                    if let Some(schema) = catalog.get_table(&table_name) {
                        for col in columns {
                            if schema.get_column(col).is_none() {
                                return Err(format!(
                                    "column '{}' not found in table '{}'",
                                    col, table_name
                                ));
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn get_table_name(&self, plan: &PhysicalPlan) -> Option<String> {
        match plan {
            PhysicalPlan::SeqScan { table, .. } => Some(table.clone()),
            PhysicalPlan::Filter { input, .. } => self.get_table_name(input),
            PhysicalPlan::Project { input, .. } => self.get_table_name(input),
        }
    }

    fn validate_expr(&self, expr: &Expr, schema: &TableSchema) -> Result<(), String> {
        match expr {
            Expr::Literal(_) => Ok(()),
            Expr::Column(name) => {
                if schema.get_column(name).is_none() {
                    Err(format!(
                        "column '{}' not found in table '{}'",
                        name, schema.name
                    ))
                } else {
                    Ok(())
                }
            }
            Expr::BinaryOp { left, right, op } => {
                self.validate_expr(left, schema)?;
                self.validate_expr(right, schema)?;
                self.validate_binary_op_types(left, right, *op, schema)
            }
        }
    }

    fn validate_binary_op_types(
        &self,
        left: &Expr,
        right: &Expr,
        op: BinaryOperator,
        schema: &TableSchema,
    ) -> Result<(), String> {
        let left_type = self.infer_type(left, schema);
        let right_type = self.infer_type(right, schema);

        match (left_type, right_type) {
            (Some(lt), Some(rt)) => {
                if !Self::types_compatible(&lt, &rt, op) {
                    return Err(format!(
                        "incompatible types for {:?}: {} and {}",
                        op, lt, rt
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn infer_type(&self, expr: &Expr, schema: &TableSchema) -> Option<DataType> {
        match expr {
            Expr::Literal(v) => match v {
                Value::Int(_) => Some(DataType::Int64),
                Value::String(_) => Some(DataType::String),
                Value::Bool(_) => Some(DataType::Bool),
                Value::Bytes(_) => Some(DataType::Bytes),
                Value::Null => None,
            },
            Expr::Column(name) => schema.get_column(name).map(|c| c.data_type),
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Eq
                | BinaryOperator::Ne
                | BinaryOperator::Lt
                | BinaryOperator::Le
                | BinaryOperator::Gt
                | BinaryOperator::Ge
                | BinaryOperator::And
                | BinaryOperator::Or => Some(DataType::Bool),
            },
        }
    }

    fn types_compatible(left: &DataType, right: &DataType, _op: BinaryOperator) -> bool {
        left == right
    }

    pub fn execute(&self, plan: PhysicalPlan) -> Result<Vec<Row>, String> {
        self.validate_plan(&plan)?;

        match plan {
            PhysicalPlan::SeqScan { table, filter } => self.execute_scan(&table, filter),
            PhysicalPlan::Filter { input, predicate } => {
                let rows = self.execute(*input)?;
                Ok(rows
                    .into_iter()
                    .filter(|row| {
                        self.eval_expr(&predicate, row)
                            .map(|v| v.as_bool().unwrap_or(false))
                            .unwrap_or(false)
                    })
                    .collect())
            }
            PhysicalPlan::Project { input, columns } => {
                let rows = self.execute(*input)?;
                Ok(rows
                    .into_iter()
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
