use crate::expr::{AggregateFunc, BinaryOperator, Expr, Value};
use crate::join::{HashJoin, NestedLoopJoin, SortMergeJoin};
use crate::plan::PhysicalPlan;
use middb_core::catalog::Catalog;
use middb_core::Database;
use std::cmp::Ordering;
use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;

pub struct Executor {
    tables: HashMap<String, Table>,
    catalog: Option<Arc<RwLock<Catalog>>>,
    database: Option<Arc<Database>>,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            tables: HashMap::new(),
            catalog: None,
            database: None,
        }
    }

    pub fn with_catalog(catalog: Arc<RwLock<Catalog>>) -> Self {
        Executor {
            tables: HashMap::new(),
            catalog: Some(catalog),
            database: None,
        }
    }

    pub fn with_database(db: Arc<Database>) -> Self {
        let catalog = db.catalog();
        Executor {
            tables: HashMap::new(),
            catalog: Some(catalog),
            database: Some(db),
        }
    }

    pub fn set_catalog(&mut self, catalog: Arc<RwLock<Catalog>>) {
        self.catalog = Some(catalog);
    }

    pub fn set_database(&mut self, db: Arc<Database>) {
        self.catalog = Some(db.catalog());
        self.database = Some(db);
    }

    pub fn register_table(&mut self, name: String, table: Table) {
        self.tables.insert(name, table);
    }

    pub fn execute(&self, plan: PhysicalPlan) -> Result<Vec<Row>, String> {
        match plan {
            PhysicalPlan::SeqScan { table, filter } => self.execute_scan(&table, filter),
            PhysicalPlan::IndexScan {
                table,
                filter,
                ..
            } => self.execute_scan(&table, filter),
            PhysicalPlan::Filter { input, predicate } => {
                let rows = self.execute(*input)?;
                Ok(rows
                    .into_iter()
                    .filter(|row| {
                        self.eval_expr(&predicate, row)
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .collect())
            }
            PhysicalPlan::Project { input, columns } => {
                let rows = self.execute(*input)?;
                if columns.len() == 1 && columns[0] == Expr::Wildcard {
                    return Ok(rows);
                }
                Ok(rows
                    .into_iter()
                    .map(|row| self.project_row(row, &columns))
                    .collect())
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
            } => {
                let left_rows = self.execute(*left)?;
                let right_rows = self.execute(*right)?;
                Ok(NestedLoopJoin::execute(
                    &left_rows,
                    &right_rows,
                    join_type,
                    condition.as_ref(),
                    &|expr, row| self.eval_expr(expr, row),
                ))
            }
            PhysicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_key,
                right_key,
            } => {
                let left_rows = self.execute(*left)?;
                let right_rows = self.execute(*right)?;
                Ok(HashJoin::execute(
                    &left_rows,
                    &right_rows,
                    join_type,
                    &left_key,
                    &right_key,
                    &|expr, row| self.eval_expr(expr, row),
                ))
            }
            PhysicalPlan::SortMergeJoin {
                left,
                right,
                join_type,
                left_key,
                right_key,
            } => {
                let left_rows = self.execute(*left)?;
                let right_rows = self.execute(*right)?;
                Ok(SortMergeJoin::execute(
                    &left_rows,
                    &right_rows,
                    join_type,
                    &left_key,
                    &right_key,
                    &|expr, row| self.eval_expr(expr, row),
                ))
            }
            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
            } => {
                let rows = self.execute(*input)?;
                self.execute_aggregate(&rows, &group_by, &aggregates)
            }
            PhysicalPlan::Sort { input, order_by } => {
                let mut rows = self.execute(*input)?;
                rows.sort_by(|a, b| {
                    for (expr, asc) in &order_by {
                        let va = self.eval_expr(expr, a);
                        let vb = self.eval_expr(expr, b);
                        let ord = match (va, vb) {
                            (Some(va), Some(vb)) => va.compare(&vb).unwrap_or(Ordering::Equal),
                            (Some(_), None) => Ordering::Less,
                            (None, Some(_)) => Ordering::Greater,
                            (None, None) => Ordering::Equal,
                        };
                        let ord = if *asc { ord } else { ord.reverse() };
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    Ordering::Equal
                });
                Ok(rows)
            }
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let rows = self.execute(*input)?;
                Ok(rows.into_iter().skip(offset).take(limit).collect())
            }
            PhysicalPlan::Insert {
                table,
                columns,
                values,
            } => self.execute_insert(&table, &columns, &values),
            PhysicalPlan::Update {
                table,
                assignments,
                filter,
            } => self.execute_update(&table, &assignments, filter),
            PhysicalPlan::Delete { table, filter } => self.execute_delete(&table, filter),
            PhysicalPlan::CreateTable {
                table,
                columns,
                if_not_exists,
            } => self.execute_create_table(&table, &columns, if_not_exists),
            PhysicalPlan::DropTable { table, if_exists } => {
                self.execute_drop_table(&table, if_exists)
            }
        }
    }

    fn execute_scan(&self, table_name: &str, filter: Option<Expr>) -> Result<Vec<Row>, String> {
        if let Some(table) = self.tables.get(table_name) {
            let mut rows = table.rows.clone();
            if let Some(predicate) = filter {
                rows.retain(|row| {
                    self.eval_expr(&predicate, row)
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                });
            }
            return Ok(rows);
        }

        if let Some(db) = &self.database {
            return self.scan_from_database(db, table_name, filter);
        }

        Err(format!("Table not found: {table_name}"))
    }

    fn scan_from_database(
        &self,
        db: &Database,
        table_name: &str,
        filter: Option<Expr>,
    ) -> Result<Vec<Row>, String> {
        let catalog = self.catalog.as_ref().ok_or("No catalog available")?;
        let catalog = catalog.read();
        let schema = catalog
            .get_table(table_name)
            .ok_or_else(|| format!("Table not found: {table_name}"))?
            .clone();
        drop(catalog);

        let prefix = format!("{table_name}/");
        let prefix_bytes = prefix.as_bytes().to_vec();
        let end_prefix = {
            let mut end = prefix_bytes.clone();
            if let Some(last) = end.last_mut() {
                *last += 1;
            }
            end
        };

        let mut rows = Vec::new();
        let iter_result = db.scan(&prefix_bytes, &end_prefix);
        for (_, value) in iter_result {
            if let Ok(row) = decode_row(&value, &schema) {
                if let Some(ref pred) = filter {
                    if self
                        .eval_expr(pred, &row)
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        rows.push(row);
                    }
                } else {
                    rows.push(row);
                }
            }
        }

        Ok(rows)
    }

    fn execute_aggregate(
        &self,
        rows: &[Row],
        group_by: &[Expr],
        aggregates: &[(AggregateFunc, Expr, String)],
    ) -> Result<Vec<Row>, String> {
        let mut groups: HashMap<Vec<Vec<u8>>, Vec<&Row>> = HashMap::new();

        for row in rows {
            let key: Vec<Vec<u8>> = group_by
                .iter()
                .map(|expr| {
                    self.eval_expr(expr, row)
                        .map(|v| v.to_sort_key())
                        .unwrap_or_default()
                })
                .collect();
            groups.entry(key).or_default().push(row);
        }

        if groups.is_empty() && group_by.is_empty() {
            groups.insert(vec![], vec![]);
        }

        let mut result_rows = Vec::new();

        for group_rows in groups.values() {
            let mut columns = Vec::new();

            for (i, expr) in group_by.iter().enumerate() {
                let col_name = match expr {
                    Expr::Column(name) => name.clone(),
                    _ => format!("group_{i}"),
                };
                let val = if let Some(first_row) = group_rows.first() {
                    self.eval_expr(expr, first_row).unwrap_or(Value::Null)
                } else {
                    Value::Null
                };
                columns.push((col_name, val));
            }

            for (func, arg, alias) in aggregates {
                let val = self.compute_aggregate(*func, arg, group_rows);
                columns.push((alias.clone(), val));
            }

            result_rows.push(Row::new_with_values(columns));
        }

        Ok(result_rows)
    }

    fn compute_aggregate(
        &self,
        func: AggregateFunc,
        arg: &Expr,
        rows: &[&Row],
    ) -> Value {
        match func {
            AggregateFunc::Count => {
                if *arg == Expr::Wildcard {
                    Value::Int(rows.len() as i64)
                } else {
                    let count = rows
                        .iter()
                        .filter(|r| {
                            self.eval_expr(arg, r)
                                .map(|v| !matches!(v, Value::Null))
                                .unwrap_or(false)
                        })
                        .count();
                    Value::Int(count as i64)
                }
            }
            AggregateFunc::Sum => {
                let mut sum = 0i64;
                let mut has_float = false;
                let mut fsum = 0.0f64;
                for row in rows {
                    if let Some(val) = self.eval_expr(arg, row) {
                        match val {
                            Value::Int(i) => {
                                sum += i;
                                fsum += i as f64;
                            }
                            Value::Float(f) => {
                                has_float = true;
                                fsum += f;
                            }
                            _ => {}
                        }
                    }
                }
                if has_float {
                    Value::Float(fsum)
                } else {
                    Value::Int(sum)
                }
            }
            AggregateFunc::Avg => {
                let mut sum = 0.0f64;
                let mut count = 0u64;
                for row in rows {
                    if let Some(val) = self.eval_expr(arg, row) {
                        match val {
                            Value::Int(i) => {
                                sum += i as f64;
                                count += 1;
                            }
                            Value::Float(f) => {
                                sum += f;
                                count += 1;
                            }
                            _ => {}
                        }
                    }
                }
                if count == 0 {
                    Value::Null
                } else {
                    Value::Float(sum / count as f64)
                }
            }
            AggregateFunc::Min => {
                let mut min: Option<Value> = None;
                for row in rows {
                    if let Some(val) = self.eval_expr(arg, row) {
                        if matches!(val, Value::Null) {
                            continue;
                        }
                        min = Some(match min {
                            None => val,
                            Some(cur) => {
                                if val.compare(&cur) == Some(Ordering::Less) {
                                    val
                                } else {
                                    cur
                                }
                            }
                        });
                    }
                }
                min.unwrap_or(Value::Null)
            }
            AggregateFunc::Max => {
                let mut max: Option<Value> = None;
                for row in rows {
                    if let Some(val) = self.eval_expr(arg, row) {
                        if matches!(val, Value::Null) {
                            continue;
                        }
                        max = Some(match max {
                            None => val,
                            Some(cur) => {
                                if val.compare(&cur) == Some(Ordering::Greater) {
                                    val
                                } else {
                                    cur
                                }
                            }
                        });
                    }
                }
                max.unwrap_or(Value::Null)
            }
        }
    }

    fn execute_insert(
        &self,
        table_name: &str,
        columns: &[String],
        value_rows: &[Vec<Expr>],
    ) -> Result<Vec<Row>, String> {
        let db = self.database.as_ref().ok_or("No database for DML")?;
        let catalog = self.catalog.as_ref().ok_or("No catalog")?;

        let schema = {
            let cat = catalog.read();
            cat.get_table(table_name)
                .ok_or_else(|| format!("Table not found: {table_name}"))?
                .clone()
        };

        let col_names: Vec<String> = if columns.is_empty() {
            schema.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            columns.to_vec()
        };

        let mut count = 0u64;
        for row_exprs in value_rows {
            if row_exprs.len() != col_names.len() {
                return Err(format!(
                    "Column count mismatch: expected {}, got {}",
                    col_names.len(),
                    row_exprs.len()
                ));
            }

            let mut row_values = Vec::new();
            for expr in row_exprs {
                let val = self
                    .eval_expr(expr, &Row::empty())
                    .ok_or("Cannot evaluate INSERT value")?;
                row_values.push(val);
            }

            let pk_val = &row_values[0];
            let key = format!("{}/{}", table_name, pk_value_to_string(pk_val));
            let encoded = encode_row(&col_names, &row_values);

            db.put(key.into_bytes(), encoded)
                .map_err(|e| format!("Insert failed: {e}"))?;
            count += 1;
        }

        Ok(vec![Row::new_with_values(vec![(
            "rows_affected".into(),
            Value::Int(count as i64),
        )])])
    }

    fn execute_update(
        &self,
        table_name: &str,
        assignments: &[(String, Expr)],
        filter: Option<Expr>,
    ) -> Result<Vec<Row>, String> {
        let mut rows = self.execute_scan(table_name, filter)?;
        let db = self.database.as_ref().ok_or("No database for DML")?;

        let catalog = self.catalog.as_ref().ok_or("No catalog")?;
        let schema = {
            let cat = catalog.read();
            cat.get_table(table_name)
                .ok_or_else(|| format!("Table not found: {table_name}"))?
                .clone()
        };
        let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

        let mut count = 0;
        for row in &mut rows {
            for (col, expr) in assignments {
                let val = self.eval_expr(expr, row).ok_or("Cannot evaluate UPDATE value")?;
                row.set_column(col.clone(), val);
            }

            let pk_val = row
                .get_column(&col_names[0])
                .unwrap_or(Value::Null);
            let key = format!("{}/{}", table_name, pk_value_to_string(&pk_val));
            let values: Vec<Value> = col_names
                .iter()
                .map(|c| row.get_column(c).unwrap_or(Value::Null))
                .collect();
            let encoded = encode_row(&col_names, &values);

            db.put(key.into_bytes(), encoded)
                .map_err(|e| format!("Update failed: {e}"))?;
            count += 1;
        }

        Ok(vec![Row::new_with_values(vec![(
            "rows_affected".into(),
            Value::Int(count),
        )])])
    }

    fn execute_delete(
        &self,
        table_name: &str,
        filter: Option<Expr>,
    ) -> Result<Vec<Row>, String> {
        let rows = self.execute_scan(table_name, filter)?;
        let db = self.database.as_ref().ok_or("No database for DML")?;

        let catalog = self.catalog.as_ref().ok_or("No catalog")?;
        let schema = {
            let cat = catalog.read();
            cat.get_table(table_name)
                .ok_or_else(|| format!("Table not found: {table_name}"))?
                .clone()
        };
        let pk_col = &schema.columns[0].name;

        let mut count = 0;
        for row in &rows {
            let pk_val = row.get_column(pk_col).unwrap_or(Value::Null);
            let key = format!("{}/{}", table_name, pk_value_to_string(&pk_val));
            db.delete(key.into_bytes())
                .map_err(|e| format!("Delete failed: {e}"))?;
            count += 1;
        }

        Ok(vec![Row::new_with_values(vec![(
            "rows_affected".into(),
            Value::Int(count),
        )])])
    }

    fn execute_create_table(
        &self,
        table_name: &str,
        columns: &[(String, String)],
        if_not_exists: bool,
    ) -> Result<Vec<Row>, String> {
        let db = self.database.as_ref().ok_or("No database for DDL")?;
        let catalog = self.catalog.as_ref().ok_or("No catalog")?;

        {
            let cat = catalog.read();
            if cat.table_exists(table_name) {
                if if_not_exists {
                    return Ok(vec![]);
                }
                return Err(format!("Table already exists: {table_name}"));
            }
        }

        let mut builder = middb_core::TableSchemaBuilder::new(table_name);
        for (col_name, col_type) in columns {
            let dt = parse_data_type(col_type)?;
            builder = builder.column(col_name, dt, true);
        }
        let schema = builder.build();

        db.create_table(schema)
            .map_err(|e| format!("Create table failed: {e}"))?;

        Ok(vec![Row::new_with_values(vec![(
            "result".into(),
            Value::String("OK".into()),
        )])])
    }

    fn execute_drop_table(&self, table_name: &str, if_exists: bool) -> Result<Vec<Row>, String> {
        let db = self.database.as_ref().ok_or("No database for DDL")?;

        match db.drop_table(table_name) {
            Ok(_) => Ok(vec![Row::new_with_values(vec![(
                "result".into(),
                Value::String("OK".into()),
            )])]),
            Err(e) => {
                if if_exists {
                    Ok(vec![])
                } else {
                    Err(format!("Drop table failed: {e}"))
                }
            }
        }
    }

    pub fn eval_expr(&self, expr: &Expr, row: &Row) -> Option<Value> {
        match expr {
            Expr::Literal(value) => Some(value.clone()),
            Expr::Column(name) => row.get_column(name),
            Expr::BinaryOp { op, left, right } => {
                let left_val = self.eval_expr(left, row)?;
                let right_val = self.eval_expr(right, row)?;
                self.eval_binary_op(*op, left_val, right_val)
            }
            Expr::IsNull(inner) => {
                let val = self.eval_expr(inner, row);
                Some(Value::Bool(val.is_none() || val == Some(Value::Null)))
            }
            Expr::IsNotNull(inner) => {
                let val = self.eval_expr(inner, row);
                Some(Value::Bool(val.is_some() && val != Some(Value::Null)))
            }
            Expr::UnaryNot(inner) => {
                let val = self.eval_expr(inner, row)?;
                val.as_bool().map(|b| Value::Bool(!b))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let val = self.eval_expr(expr, row)?;
                let found = list.iter().any(|item| {
                    self.eval_expr(item, row)
                        .map(|v| v == val)
                        .unwrap_or(false)
                });
                Some(Value::Bool(if *negated { !found } else { found }))
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let val = self.eval_expr(expr, row)?;
                let low_val = self.eval_expr(low, row)?;
                let high_val = self.eval_expr(high, row)?;
                let in_range = val.compare(&low_val).map(|o| o != Ordering::Less).unwrap_or(false)
                    && val
                        .compare(&high_val)
                        .map(|o| o != Ordering::Greater)
                        .unwrap_or(false);
                Some(Value::Bool(if *negated { !in_range } else { in_range }))
            }
            Expr::Wildcard => None,
            Expr::Aggregate { .. } => None,
        }
    }

    fn eval_binary_op(&self, op: BinaryOperator, left: Value, right: Value) -> Option<Value> {
        match op {
            BinaryOperator::Eq => Some(Value::Bool(left == right)),
            BinaryOperator::Ne => Some(Value::Bool(left != right)),
            BinaryOperator::Lt => {
                left.compare(&right).map(|ord| Value::Bool(ord == Ordering::Less))
            }
            BinaryOperator::Le => {
                left.compare(&right)
                    .map(|ord| Value::Bool(ord != Ordering::Greater))
            }
            BinaryOperator::Gt => {
                left.compare(&right)
                    .map(|ord| Value::Bool(ord == Ordering::Greater))
            }
            BinaryOperator::Ge => {
                left.compare(&right)
                    .map(|ord| Value::Bool(ord != Ordering::Less))
            }
            BinaryOperator::And => match (left.as_bool(), right.as_bool()) {
                (Some(a), Some(b)) => Some(Value::Bool(a && b)),
                _ => None,
            },
            BinaryOperator::Or => match (left.as_bool(), right.as_bool()) {
                (Some(a), Some(b)) => Some(Value::Bool(a || b)),
                _ => None,
            },
            BinaryOperator::Add => match (&left, &right) {
                (Value::Int(a), Value::Int(b)) => Some(Value::Int(a + b)),
                (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
                (Value::Int(a), Value::Float(b)) => Some(Value::Float(*a as f64 + b)),
                (Value::Float(a), Value::Int(b)) => Some(Value::Float(a + *b as f64)),
                _ => None,
            },
            BinaryOperator::Sub => match (&left, &right) {
                (Value::Int(a), Value::Int(b)) => Some(Value::Int(a - b)),
                (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
                _ => None,
            },
            BinaryOperator::Mul => match (&left, &right) {
                (Value::Int(a), Value::Int(b)) => Some(Value::Int(a * b)),
                (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
                _ => None,
            },
            BinaryOperator::Div => match (&left, &right) {
                (Value::Int(a), Value::Int(b)) if *b != 0 => Some(Value::Int(a / b)),
                (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a / b)),
                _ => None,
            },
            BinaryOperator::Mod => match (&left, &right) {
                (Value::Int(a), Value::Int(b)) if *b != 0 => Some(Value::Int(a % b)),
                _ => None,
            },
        }
    }

    fn project_row(&self, row: Row, columns: &[Expr]) -> Row {
        let mut fields = Vec::new();
        for col in columns {
            match col {
                Expr::Column(name) => {
                    let val = row.get_column(name).unwrap_or(Value::Null);
                    fields.push((name.clone(), val));
                }
                Expr::Wildcard => {
                    for (k, v) in row.columns_map() {
                        fields.push((k.clone(), v.clone()));
                    }
                }
                other => {
                    let val = self.eval_expr(other, &row).unwrap_or(Value::Null);
                    fields.push((format!("{other}"), val));
                }
            }
        }
        Row::new_with_values(fields)
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
    column_order: Vec<String>,
}

impl Row {
    pub fn new_with_values(columns: Vec<(String, Value)>) -> Self {
        let column_order: Vec<String> = columns.iter().map(|(k, _)| k.clone()).collect();
        let map = columns.into_iter().collect();
        Row {
            columns: map,
            column_order,
        }
    }

    pub fn new(fields: Vec<Value>) -> Self {
        let pairs: Vec<(String, Value)> = fields
            .into_iter()
            .enumerate()
            .map(|(i, v)| (format!("col{i}"), v))
            .collect();
        Self::new_with_values(pairs)
    }

    pub fn empty() -> Self {
        Row {
            columns: HashMap::new(),
            column_order: Vec::new(),
        }
    }

    pub fn from_map(columns: HashMap<String, Value>) -> Self {
        let column_order: Vec<String> = columns.keys().cloned().collect();
        Row {
            columns,
            column_order,
        }
    }

    pub fn get_column(&self, name: &str) -> Option<Value> {
        self.columns.get(name).cloned()
    }

    pub fn set_column(&mut self, name: String, value: Value) {
        if !self.columns.contains_key(&name) {
            self.column_order.push(name.clone());
        }
        self.columns.insert(name, value);
    }

    pub fn columns_map(&self) -> &HashMap<String, Value> {
        &self.columns
    }

    pub fn column_order(&self) -> &[String] {
        &self.column_order
    }

    pub fn fields(&self) -> Vec<Value> {
        self.column_order
            .iter()
            .filter_map(|k| self.columns.get(k).cloned())
            .collect()
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

fn pk_value_to_string(val: &Value) -> String {
    match val {
        Value::Int(i) => format!("{i:020}"),
        Value::String(s) => s.clone(),
        Value::Float(f) => format!("{f}"),
        Value::Bool(b) => format!("{b}"),
        Value::Bytes(b) => hex::encode(b),
        Value::Null => "NULL".to_string(),
    }
}

mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }
}

fn encode_row(col_names: &[String], values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    let count = col_names.len() as u16;
    buf.extend_from_slice(&count.to_le_bytes());

    for (i, val) in values.iter().enumerate() {
        let name = &col_names[i];
        let name_bytes = name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        match val {
            Value::Int(n) => {
                buf.push(1);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(2);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::String(s) => {
                buf.push(3);
                let s_bytes = s.as_bytes();
                buf.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(s_bytes);
            }
            Value::Bool(b) => {
                buf.push(4);
                buf.push(*b as u8);
            }
            Value::Bytes(b) => {
                buf.push(5);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Null => {
                buf.push(0);
            }
        }
    }

    buf
}

fn decode_row(
    data: &[u8],
    schema: &middb_core::catalog::TableSchema,
) -> Result<Row, String> {
    if data.len() < 2 {
        return Err("Row data too short".into());
    }

    let count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let mut offset = 2;
    let mut columns = Vec::with_capacity(count);

    for _ in 0..count {
        if offset + 2 > data.len() {
            return Err("Truncated row".into());
        }
        let name_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + name_len > data.len() {
            return Err("Truncated row name".into());
        }
        let name = String::from_utf8_lossy(&data[offset..offset + name_len]).to_string();
        offset += name_len;

        if offset >= data.len() {
            return Err("Truncated row value type".into());
        }
        let type_byte = data[offset];
        offset += 1;

        let val = match type_byte {
            0 => Value::Null,
            1 => {
                if offset + 8 > data.len() {
                    return Err("Truncated int value".into());
                }
                let n = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Value::Int(n)
            }
            2 => {
                if offset + 8 > data.len() {
                    return Err("Truncated float value".into());
                }
                let f = f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                offset += 8;
                Value::Float(f)
            }
            3 => {
                if offset + 4 > data.len() {
                    return Err("Truncated string length".into());
                }
                let len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err("Truncated string value".into());
                }
                let s = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
                offset += len;
                Value::String(s)
            }
            4 => {
                if offset >= data.len() {
                    return Err("Truncated bool value".into());
                }
                let b = data[offset] != 0;
                offset += 1;
                Value::Bool(b)
            }
            5 => {
                if offset + 4 > data.len() {
                    return Err("Truncated bytes length".into());
                }
                let len =
                    u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
                offset += 4;
                if offset + len > data.len() {
                    return Err("Truncated bytes value".into());
                }
                let b = data[offset..offset + len].to_vec();
                offset += len;
                Value::Bytes(b)
            }
            _ => return Err(format!("Unknown value type: {type_byte}")),
        };

        columns.push((name, val));
    }

    let _ = schema;
    Ok(Row::new_with_values(columns))
}

fn parse_data_type(s: &str) -> Result<middb_core::DataType, String> {
    let upper = s.to_uppercase();
    match upper.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "INT64" => Ok(middb_core::DataType::Int64),
        "BOOL" | "BOOLEAN" => Ok(middb_core::DataType::Bool),
        "BLOB" | "BYTES" | "BYTEA" => Ok(middb_core::DataType::Bytes),
        s if s.starts_with("VARCHAR") || s.starts_with("TEXT") || s.starts_with("STRING") || s == "CHAR" => {
            Ok(middb_core::DataType::String)
        }
        "FLOAT" | "DOUBLE" | "REAL" | "FLOAT64" => Ok(middb_core::DataType::String),
        _ => Err(format!("Unknown data type: {s}")),
    }
}
