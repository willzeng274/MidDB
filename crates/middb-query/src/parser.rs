use crate::expr::{AggregateFunc, BinaryOperator, Expr, Value};
use crate::plan::{JoinType, LogicalPlan};
use sqlparser::ast::{self, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct SqlParser;

impl SqlParser {
    pub fn parse(sql: &str) -> Result<LogicalPlan, String> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {}", e))?;

        if statements.is_empty() {
            return Err("Empty SQL statement".into());
        }
        if statements.len() > 1 {
            return Err("Multiple statements not supported".into());
        }

        Self::convert_statement(statements.into_iter().next().unwrap())
    }

    fn convert_statement(stmt: Statement) -> Result<LogicalPlan, String> {
        match stmt {
            Statement::Query(query) => Self::convert_query(*query),
            Statement::Insert(insert) => Self::convert_insert(insert),
            Statement::Update { table, assignments, selection, .. } => {
                Self::convert_update(table, assignments, selection)
            }
            Statement::Delete(delete) => Self::convert_delete(delete),
            Statement::CreateTable(create) => Self::convert_create_table(create),
            Statement::Drop { object_type, names, if_exists, .. } => {
                Self::convert_drop(object_type, names, if_exists)
            }
            _ => Err(format!("Unsupported statement type")),
        }
    }

    fn convert_query(query: ast::Query) -> Result<LogicalPlan, String> {
        let body = match *query.body {
            ast::SetExpr::Select(select) => select,
            _ => return Err("Only SELECT queries supported".into()),
        };

        let mut plan = Self::convert_from(&body.from)?;

        if let Some(selection) = body.selection {
            let predicate = Self::convert_expr(selection)?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        let has_aggregates = body.projection.iter().any(|item| {
            if let ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } = item {
                Self::expr_has_aggregate(expr)
            } else {
                false
            }
        });

        let group_by_exprs = match &body.group_by {
            ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            _ => vec![],
        };

        if has_aggregates || !group_by_exprs.is_empty() {
            let group_by: Vec<Expr> = group_by_exprs
                .iter()
                .map(|e| Self::convert_expr(e.clone()))
                .collect::<Result<_, _>>()?;

            let mut aggregates = Vec::new();
            let mut projections = Vec::new();

            for (i, item) in body.projection.iter().enumerate() {
                match item {
                    ast::SelectItem::UnnamedExpr(expr) => {
                        if let Some((func, arg)) = Self::extract_aggregate(expr) {
                            let alias = format!("agg_{}", i);
                            aggregates.push((func, arg, alias.clone()));
                            projections.push(Expr::Column(alias));
                        } else {
                            projections.push(Self::convert_expr(expr.clone())?);
                        }
                    }
                    ast::SelectItem::ExprWithAlias { expr, alias } => {
                        if let Some((func, arg)) = Self::extract_aggregate(expr) {
                            let alias_str = alias.value.clone();
                            aggregates.push((func, arg, alias_str.clone()));
                            projections.push(Expr::Column(alias_str));
                        } else {
                            projections.push(Self::convert_expr(expr.clone())?);
                        }
                    }
                    ast::SelectItem::Wildcard(_) => {
                        projections.push(Expr::Wildcard);
                    }
                    _ => return Err("Unsupported select item".into()),
                }
            }

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggregates,
            };

            let is_all_wildcard = projections.len() == 1 && projections[0] == Expr::Wildcard;
            if !is_all_wildcard {
                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns: projections,
                };
            }
        } else {
            let is_wildcard = body.projection.len() == 1
                && matches!(&body.projection[0], ast::SelectItem::Wildcard(_));

            if !is_wildcard {
                let columns: Vec<Expr> = body
                    .projection
                    .into_iter()
                    .map(|item| match item {
                        ast::SelectItem::UnnamedExpr(expr) => Self::convert_expr(expr),
                        ast::SelectItem::ExprWithAlias { expr, .. } => Self::convert_expr(expr),
                        ast::SelectItem::Wildcard(_) => Ok(Expr::Wildcard),
                        _ => Err("Unsupported select item".into()),
                    })
                    .collect::<Result<_, _>>()?;

                plan = LogicalPlan::Project {
                    input: Box::new(plan),
                    columns,
                };
            }
        }

        if let Some(ref order_by) = query.order_by {
            let order_by_exprs: Vec<(Expr, bool)> = order_by
                .exprs
                .iter()
                .map(|o| {
                    let expr = Self::convert_expr(o.expr.clone())?;
                    let asc = o.asc.unwrap_or(true);
                    Ok((expr, asc))
                })
                .collect::<Result<_, String>>()?;

            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: order_by_exprs,
            };
        }

        if let Some(limit_expr) = query.limit {
            let limit = Self::expr_to_usize(&limit_expr)?;
            let offset = query
                .offset
                .map(|o| Self::expr_to_usize(&o.value))
                .transpose()?
                .unwrap_or(0);

            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

        Ok(plan)
    }

    fn convert_from(from: &[ast::TableWithJoins]) -> Result<LogicalPlan, String> {
        if from.is_empty() {
            return Err("No FROM clause".into());
        }

        let first = &from[0];
        let mut plan = Self::convert_table_factor(&first.relation)?;

        for join in &first.joins {
            let right = Self::convert_table_factor(&join.relation)?;
            let (join_type, condition) = Self::convert_join_constraint(&join.join_operator)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type,
                condition,
            };
        }

        for twj in &from[1..] {
            let right = Self::convert_table_factor(&twj.relation)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type: JoinType::Cross,
                condition: None,
            };
        }

        Ok(plan)
    }

    fn convert_table_factor(table: &ast::TableFactor) -> Result<LogicalPlan, String> {
        match table {
            ast::TableFactor::Table { name, .. } => {
                let table_name = name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                Ok(LogicalPlan::Scan {
                    table: table_name,
                    filter: None,
                })
            }
            _ => Err("Only simple table references supported".into()),
        }
    }

    fn convert_join_constraint(
        op: &ast::JoinOperator,
    ) -> Result<(JoinType, Option<Expr>), String> {
        match op {
            ast::JoinOperator::Inner(constraint) => {
                let cond = Self::extract_join_condition(constraint)?;
                Ok((JoinType::Inner, cond))
            }
            ast::JoinOperator::LeftOuter(constraint) => {
                let cond = Self::extract_join_condition(constraint)?;
                Ok((JoinType::Left, cond))
            }
            ast::JoinOperator::RightOuter(constraint) => {
                let cond = Self::extract_join_condition(constraint)?;
                Ok((JoinType::Right, cond))
            }
            ast::JoinOperator::CrossJoin => Ok((JoinType::Cross, None)),
            _ => Err("Unsupported join type".into()),
        }
    }

    fn extract_join_condition(constraint: &ast::JoinConstraint) -> Result<Option<Expr>, String> {
        match constraint {
            ast::JoinConstraint::On(expr) => Ok(Some(Self::convert_expr(expr.clone())?)),
            ast::JoinConstraint::None => Ok(None),
            _ => Err("Unsupported join constraint".into()),
        }
    }

    fn convert_expr(expr: ast::Expr) -> Result<Expr, String> {
        match expr {
            ast::Expr::Identifier(ident) => Ok(Expr::Column(ident.value)),
            ast::Expr::CompoundIdentifier(idents) => {
                let name = idents.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
                Ok(Expr::Column(name))
            }
            ast::Expr::Value(val) => Self::convert_value(val),
            ast::Expr::BinaryOp { left, op, right } => {
                let left = Self::convert_expr(*left)?;
                let right = Self::convert_expr(*right)?;
                let op = Self::convert_binary_op(op)?;
                Ok(Expr::BinaryOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr,
            } => {
                let inner = Self::convert_expr(*expr)?;
                Ok(Expr::UnaryNot(Box::new(inner)))
            }
            ast::Expr::IsNull(expr) => {
                let inner = Self::convert_expr(*expr)?;
                Ok(Expr::IsNull(Box::new(inner)))
            }
            ast::Expr::IsNotNull(expr) => {
                let inner = Self::convert_expr(*expr)?;
                Ok(Expr::IsNotNull(Box::new(inner)))
            }
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let e = Self::convert_expr(*expr)?;
                let l = Self::convert_expr(*low)?;
                let h = Self::convert_expr(*high)?;
                Ok(Expr::Between {
                    expr: Box::new(e),
                    low: Box::new(l),
                    high: Box::new(h),
                    negated,
                })
            }
            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let e = Self::convert_expr(*expr)?;
                let l: Vec<Expr> = list
                    .into_iter()
                    .map(Self::convert_expr)
                    .collect::<Result<_, _>>()?;
                Ok(Expr::InList {
                    expr: Box::new(e),
                    list: l,
                    negated,
                })
            }
            ast::Expr::Function(func) => Self::convert_function(func),
            ast::Expr::Nested(inner) => Self::convert_expr(*inner),
            _ => Err(format!("Unsupported expression: {:?}", expr)),
        }
    }

    fn convert_value(val: ast::Value) -> Result<Expr, String> {
        match val {
            ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Expr::Literal(Value::Int(i)))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Expr::Literal(Value::Float(f)))
                } else {
                    Err(format!("Invalid number: {}", n))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Expr::Literal(Value::String(s)))
            }
            ast::Value::Boolean(b) => Ok(Expr::Literal(Value::Bool(b))),
            ast::Value::Null => Ok(Expr::Literal(Value::Null)),
            _ => Err(format!("Unsupported value type")),
        }
    }

    fn convert_binary_op(op: ast::BinaryOperator) -> Result<BinaryOperator, String> {
        match op {
            ast::BinaryOperator::Eq => Ok(BinaryOperator::Eq),
            ast::BinaryOperator::NotEq => Ok(BinaryOperator::Ne),
            ast::BinaryOperator::Lt => Ok(BinaryOperator::Lt),
            ast::BinaryOperator::LtEq => Ok(BinaryOperator::Le),
            ast::BinaryOperator::Gt => Ok(BinaryOperator::Gt),
            ast::BinaryOperator::GtEq => Ok(BinaryOperator::Ge),
            ast::BinaryOperator::And => Ok(BinaryOperator::And),
            ast::BinaryOperator::Or => Ok(BinaryOperator::Or),
            ast::BinaryOperator::Plus => Ok(BinaryOperator::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOperator::Sub),
            ast::BinaryOperator::Multiply => Ok(BinaryOperator::Mul),
            ast::BinaryOperator::Divide => Ok(BinaryOperator::Div),
            ast::BinaryOperator::Modulo => Ok(BinaryOperator::Mod),
            _ => Err(format!("Unsupported binary operator: {:?}", op)),
        }
    }

    fn convert_function(func: ast::Function) -> Result<Expr, String> {
        let name = func.name.0.iter().map(|i| i.value.to_uppercase()).collect::<Vec<_>>().join(".");
        let agg_func = match name.as_str() {
            "COUNT" => AggregateFunc::Count,
            "SUM" => AggregateFunc::Sum,
            "AVG" => AggregateFunc::Avg,
            "MIN" => AggregateFunc::Min,
            "MAX" => AggregateFunc::Max,
            _ => return Err(format!("Unknown function: {}", name)),
        };

        let args = match func.args {
            ast::FunctionArguments::List(args) => args,
            ast::FunctionArguments::None => {
                return Ok(Expr::Aggregate {
                    func: agg_func,
                    arg: Box::new(Expr::Wildcard),
                });
            }
            _ => return Err("Unsupported function argument format".into()),
        };

        if args.args.is_empty() {
            return Ok(Expr::Aggregate {
                func: agg_func,
                arg: Box::new(Expr::Wildcard),
            });
        }

        let arg = match &args.args[0] {
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                Self::convert_expr(expr.clone())?
            }
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => Expr::Wildcard,
            _ => return Err("Unsupported function argument".into()),
        };

        Ok(Expr::Aggregate {
            func: agg_func,
            arg: Box::new(arg),
        })
    }

    fn convert_insert(insert: ast::Insert) -> Result<LogicalPlan, String> {
        let table_name = insert.table_name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");

        let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

        let source = insert.source.ok_or("INSERT requires VALUES")?;
        let rows = match *source.body {
            ast::SetExpr::Values(values) => {
                values
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(Self::convert_expr)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            _ => return Err("Only VALUES clause supported for INSERT".into()),
        };

        Ok(LogicalPlan::Insert {
            table: table_name,
            columns,
            values: rows,
        })
    }

    fn convert_update(
        table: ast::TableWithJoins,
        assignments: Vec<ast::Assignment>,
        selection: Option<ast::Expr>,
    ) -> Result<LogicalPlan, String> {
        let table_name = match &table.relation {
            ast::TableFactor::Table { name, .. } => {
                name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
            }
            _ => return Err("Unsupported UPDATE table format".into()),
        };

        let assigns: Vec<(String, Expr)> = assignments
            .into_iter()
            .map(|a| {
                let col = match a.target {
                    ast::AssignmentTarget::ColumnName(name) => {
                        name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
                    }
                    ast::AssignmentTarget::Tuple(names) => {
                        names.first().map(|n| n.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")).unwrap_or_default()
                    }
                };
                let val = Self::convert_expr(a.value)?;
                Ok((col, val))
            })
            .collect::<Result<_, String>>()?;

        let filter = selection.map(Self::convert_expr).transpose()?;

        Ok(LogicalPlan::Update {
            table: table_name,
            assignments: assigns,
            filter,
        })
    }

    fn convert_delete(delete: ast::Delete) -> Result<LogicalPlan, String> {
        let tables = match &delete.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        if tables.is_empty() {
            return Err("DELETE requires FROM clause".into());
        }
        let table_name = match &tables[0].relation {
            ast::TableFactor::Table { name, .. } => {
                name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
            }
            _ => return Err("Unsupported DELETE table format".into()),
        };

        let filter = delete.selection.map(Self::convert_expr).transpose()?;

        Ok(LogicalPlan::Delete {
            table: table_name,
            filter,
        })
    }

    fn convert_create_table(create: ast::CreateTable) -> Result<LogicalPlan, String> {
        let table_name = create.name.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");

        let columns: Vec<(String, String)> = create
            .columns
            .into_iter()
            .map(|col| (col.name.value, col.data_type.to_string()))
            .collect();

        Ok(LogicalPlan::CreateTable {
            table: table_name,
            columns,
            if_not_exists: create.if_not_exists,
        })
    }

    fn convert_drop(
        object_type: ast::ObjectType,
        names: Vec<ast::ObjectName>,
        if_exists: bool,
    ) -> Result<LogicalPlan, String> {
        if object_type != ast::ObjectType::Table {
            return Err("Only DROP TABLE supported".into());
        }
        if names.is_empty() {
            return Err("DROP TABLE requires a table name".into());
        }
        let table_name = names[0].0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".");
        Ok(LogicalPlan::DropTable {
            table: table_name,
            if_exists,
        })
    }

    fn expr_has_aggregate(expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function(f) => {
                let name = f.name.0.iter().map(|i| i.value.to_uppercase()).collect::<Vec<_>>().join(".");
                matches!(name.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                Self::expr_has_aggregate(left) || Self::expr_has_aggregate(right)
            }
            ast::Expr::Nested(inner) => Self::expr_has_aggregate(inner),
            _ => false,
        }
    }

    fn extract_aggregate(expr: &ast::Expr) -> Option<(AggregateFunc, Expr)> {
        if let ast::Expr::Function(func) = expr {
            let name = func.name.0.iter().map(|i| i.value.to_uppercase()).collect::<Vec<_>>().join(".");
            let agg_func = match name.as_str() {
                "COUNT" => AggregateFunc::Count,
                "SUM" => AggregateFunc::Sum,
                "AVG" => AggregateFunc::Avg,
                "MIN" => AggregateFunc::Min,
                "MAX" => AggregateFunc::Max,
                _ => return None,
            };

            let arg = match &func.args {
                ast::FunctionArguments::List(args) => {
                    if args.args.is_empty() {
                        Expr::Wildcard
                    } else {
                        match &args.args[0] {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                Self::convert_expr(e.clone()).ok()?
                            }
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => Expr::Wildcard,
                            _ => return None,
                        }
                    }
                }
                _ => Expr::Wildcard,
            };

            Some((agg_func, arg))
        } else {
            None
        }
    }

    fn expr_to_usize(expr: &ast::Expr) -> Result<usize, String> {
        match expr {
            ast::Expr::Value(val) => match val {
                ast::Value::Number(n, _) => n
                    .parse::<usize>()
                    .map_err(|_| format!("Expected integer, got: {}", n)),
                _ => Err("Expected integer for LIMIT/OFFSET".into()),
            },
            _ => Err("Expected literal for LIMIT/OFFSET".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star() {
        let plan = SqlParser::parse("SELECT * FROM users").unwrap();
        match plan {
            LogicalPlan::Scan { table, .. } => assert_eq!(table, "users"),
            _ => panic!("Expected Scan"),
        }
    }

    #[test]
    fn test_select_with_where() {
        let plan = SqlParser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                assert!(matches!(predicate, Expr::BinaryOp { .. }));
            }
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn test_select_columns() {
        let plan = SqlParser::parse("SELECT name, age FROM users").unwrap();
        match plan {
            LogicalPlan::Project { columns, .. } => {
                assert_eq!(columns.len(), 2);
            }
            _ => panic!("Expected Project"),
        }
    }

    #[test]
    fn test_join() {
        let plan =
            SqlParser::parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
                .unwrap();
        match plan {
            LogicalPlan::Join { join_type, .. } => {
                assert_eq!(join_type, JoinType::Inner);
            }
            _ => panic!("Expected Join"),
        }
    }

    #[test]
    fn test_aggregate() {
        let plan = SqlParser::parse("SELECT COUNT(*) FROM users").unwrap();
        match plan {
            LogicalPlan::Project {
                input, columns, ..
            } => {
                assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
                assert_eq!(columns.len(), 1);
            }
            _ => panic!("Expected Project over Aggregate, got: {:?}", plan),
        }
    }

    #[test]
    fn test_group_by() {
        let plan =
            SqlParser::parse("SELECT department, COUNT(*) FROM employees GROUP BY department")
                .unwrap();
        match plan {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Aggregate { .. }));
            }
            _ => panic!("Expected Project over Aggregate"),
        }
    }

    #[test]
    fn test_order_by_limit() {
        let plan = SqlParser::parse("SELECT * FROM users ORDER BY name LIMIT 10").unwrap();
        match plan {
            LogicalPlan::Limit { limit, input, .. } => {
                assert_eq!(limit, 10);
                assert!(matches!(*input, LogicalPlan::Sort { .. }));
            }
            _ => panic!("Expected Limit"),
        }
    }

    #[test]
    fn test_insert() {
        let plan =
            SqlParser::parse("INSERT INTO users (name, age) VALUES ('Alice', 30)").unwrap();
        match plan {
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "users");
                assert_eq!(columns, vec!["name", "age"]);
                assert_eq!(values.len(), 1);
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_update() {
        let plan = SqlParser::parse("UPDATE users SET age = 31 WHERE name = 'Alice'").unwrap();
        match plan {
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => {
                assert_eq!(table, "users");
                assert_eq!(assignments.len(), 1);
                assert!(filter.is_some());
            }
            _ => panic!("Expected Update"),
        }
    }

    #[test]
    fn test_delete() {
        let plan = SqlParser::parse("DELETE FROM users WHERE id = 1").unwrap();
        match plan {
            LogicalPlan::Delete { table, filter } => {
                assert_eq!(table, "users");
                assert!(filter.is_some());
            }
            _ => panic!("Expected Delete"),
        }
    }

    #[test]
    fn test_create_table() {
        let plan = SqlParser::parse(
            "CREATE TABLE users (id INT, name VARCHAR(255), age INT)",
        )
        .unwrap();
        match plan {
            LogicalPlan::CreateTable { table, columns, .. } => {
                assert_eq!(table, "users");
                assert_eq!(columns.len(), 3);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_drop_table() {
        let plan = SqlParser::parse("DROP TABLE IF EXISTS users").unwrap();
        match plan {
            LogicalPlan::DropTable {
                table, if_exists, ..
            } => {
                assert_eq!(table, "users");
                assert!(if_exists);
            }
            _ => panic!("Expected DropTable"),
        }
    }

    #[test]
    fn test_left_join() {
        let plan = SqlParser::parse(
            "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        match plan {
            LogicalPlan::Join { join_type, .. } => {
                assert_eq!(join_type, JoinType::Left);
            }
            _ => panic!("Expected Join"),
        }
    }

    #[test]
    fn test_between() {
        let plan = SqlParser::parse("SELECT * FROM users WHERE age BETWEEN 18 AND 65").unwrap();
        match plan {
            LogicalPlan::Filter { predicate, .. } => {
                assert!(matches!(predicate, Expr::Between { .. }));
            }
            _ => panic!("Expected Filter with Between"),
        }
    }
}
