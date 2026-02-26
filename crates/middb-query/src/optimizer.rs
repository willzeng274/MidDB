use crate::expr::{BinaryOperator, Expr};
use crate::plan::{JoinType, LogicalPlan, PhysicalPlan};
use crate::statistics::StatisticsCollector;

pub struct CostBasedOptimizer {
    stats: StatisticsCollector,
}

impl CostBasedOptimizer {
    pub fn new() -> Self {
        CostBasedOptimizer {
            stats: StatisticsCollector::new(),
        }
    }

    pub fn with_stats(stats: StatisticsCollector) -> Self {
        CostBasedOptimizer { stats }
    }

    pub fn optimize(&self, plan: LogicalPlan) -> PhysicalPlan {
        let optimized = self.push_predicates_down(plan);
        self.to_physical(optimized)
    }

    fn push_predicates_down(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter {
                input,
                predicate,
            } => {
                let input = self.push_predicates_down(*input);
                match input {
                    LogicalPlan::Scan { table, filter } => {
                        let merged = match filter {
                            Some(existing) => Expr::BinaryOp {
                                op: BinaryOperator::And,
                                left: Box::new(existing),
                                right: Box::new(predicate),
                            },
                            None => predicate,
                        };
                        LogicalPlan::Scan {
                            table,
                            filter: Some(merged),
                        }
                    }
                    LogicalPlan::Join {
                        left,
                        right,
                        join_type,
                        condition,
                    } => {
                        if let Some((left_pred, right_pred, join_pred)) =
                            self.split_join_predicate(&predicate, &left, &right)
                        {
                            let new_left = if let Some(lp) = left_pred {
                                Box::new(LogicalPlan::Filter {
                                    input: left,
                                    predicate: lp,
                                })
                            } else {
                                left
                            };
                            let new_right = if let Some(rp) = right_pred {
                                Box::new(LogicalPlan::Filter {
                                    input: right,
                                    predicate: rp,
                                })
                            } else {
                                right
                            };
                            let new_cond = match (condition, join_pred) {
                                (Some(c), Some(j)) => Some(Expr::BinaryOp {
                                    op: BinaryOperator::And,
                                    left: Box::new(c),
                                    right: Box::new(j),
                                }),
                                (Some(c), None) => Some(c),
                                (None, Some(j)) => Some(j),
                                (None, None) => None,
                            };
                            LogicalPlan::Join {
                                left: new_left,
                                right: new_right,
                                join_type,
                                condition: new_cond,
                            }
                        } else {
                            LogicalPlan::Filter {
                                input: Box::new(LogicalPlan::Join {
                                    left,
                                    right,
                                    join_type,
                                    condition,
                                }),
                                predicate,
                            }
                        }
                    }
                    other => LogicalPlan::Filter {
                        input: Box::new(other),
                        predicate,
                    },
                }
            }
            LogicalPlan::Project { input, columns } => {
                let input = self.push_predicates_down(*input);
                LogicalPlan::Project {
                    input: Box::new(input),
                    columns,
                }
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let left = self.push_predicates_down(*left);
                let right = self.push_predicates_down(*right);
                LogicalPlan::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    condition,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                let input = self.push_predicates_down(*input);
                LogicalPlan::Sort {
                    input: Box::new(input),
                    order_by,
                }
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input = self.push_predicates_down(*input);
                LogicalPlan::Limit {
                    input: Box::new(input),
                    limit,
                    offset,
                }
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let input = self.push_predicates_down(*input);
                LogicalPlan::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggregates,
                }
            }
            other => other,
        }
    }

    fn to_physical(&self, plan: LogicalPlan) -> PhysicalPlan {
        match plan {
            LogicalPlan::Scan { table, filter } => PhysicalPlan::SeqScan { table, filter },
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
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => self.plan_join(*left, *right, join_type, condition),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let child = self.to_physical(*input);
                PhysicalPlan::HashAggregate {
                    input: Box::new(child),
                    group_by,
                    aggregates,
                }
            }
            LogicalPlan::Sort { input, order_by } => {
                let child = self.to_physical(*input);
                PhysicalPlan::Sort {
                    input: Box::new(child),
                    order_by,
                }
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let child = self.to_physical(*input);
                PhysicalPlan::Limit {
                    input: Box::new(child),
                    limit,
                    offset,
                }
            }
            LogicalPlan::Insert {
                table,
                columns,
                values,
            } => PhysicalPlan::Insert {
                table,
                columns,
                values,
            },
            LogicalPlan::Update {
                table,
                assignments,
                filter,
            } => PhysicalPlan::Update {
                table,
                assignments,
                filter,
            },
            LogicalPlan::Delete { table, filter } => PhysicalPlan::Delete { table, filter },
            LogicalPlan::CreateTable {
                table,
                columns,
                if_not_exists,
            } => PhysicalPlan::CreateTable {
                table,
                columns,
                if_not_exists,
            },
            LogicalPlan::DropTable { table, if_exists } => {
                PhysicalPlan::DropTable { table, if_exists }
            }
        }
    }

    fn plan_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        condition: Option<Expr>,
    ) -> PhysicalPlan {
        let left_phys = self.to_physical(left.clone());
        let right_phys = self.to_physical(right.clone());

        if join_type == JoinType::Cross || condition.is_none() {
            return PhysicalPlan::NestedLoopJoin {
                left: Box::new(left_phys),
                right: Box::new(right_phys),
                join_type,
                condition,
            };
        }

        let condition = condition.unwrap();

        if let Some((left_key, right_key)) = Self::extract_equi_join_keys(&condition) {
            let left_rows = self.estimate_rows(&left);
            let right_rows = self.estimate_rows(&right);

            if left_rows + right_rows < 100_000 {
                return PhysicalPlan::HashJoin {
                    left: Box::new(left_phys),
                    right: Box::new(right_phys),
                    join_type,
                    left_key,
                    right_key,
                };
            }

            return PhysicalPlan::SortMergeJoin {
                left: Box::new(left_phys),
                right: Box::new(right_phys),
                join_type,
                left_key,
                right_key,
            };
        }

        PhysicalPlan::NestedLoopJoin {
            left: Box::new(left_phys),
            right: Box::new(right_phys),
            join_type,
            condition: Some(condition),
        }
    }

    fn extract_equi_join_keys(condition: &Expr) -> Option<(Expr, Expr)> {
        if let Expr::BinaryOp {
            op: BinaryOperator::Eq,
            left,
            right,
        } = condition
        {
            if matches!(left.as_ref(), Expr::Column(_)) && matches!(right.as_ref(), Expr::Column(_))
            {
                return Some((*left.clone(), *right.clone()));
            }
        }
        None
    }

    fn estimate_rows(&self, plan: &LogicalPlan) -> u64 {
        match plan {
            LogicalPlan::Scan { table, filter } => {
                let base = self.stats.estimated_row_count(table);
                if filter.is_some() {
                    (base as f64 * 0.33) as u64
                } else {
                    base
                }
            }
            LogicalPlan::Filter { input, .. } => {
                (self.estimate_rows(input) as f64 * 0.33) as u64
            }
            LogicalPlan::Join { left, right, .. } => {
                let l = self.estimate_rows(left);
                let r = self.estimate_rows(right);
                (l as f64 * r as f64 * 0.1) as u64
            }
            LogicalPlan::Aggregate { input, .. } => {
                (self.estimate_rows(input) as f64 * 0.1).max(1.0) as u64
            }
            LogicalPlan::Limit { limit, .. } => *limit as u64,
            _ => 1000,
        }
    }

    fn split_join_predicate(
        &self,
        predicate: &Expr,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Option<(Option<Expr>, Option<Expr>, Option<Expr>)> {
        let left_tables = Self::collect_tables(left);
        let right_tables = Self::collect_tables(right);

        let mut left_preds = Vec::new();
        let mut right_preds = Vec::new();
        let mut join_preds = Vec::new();

        let conjuncts = Self::flatten_and(predicate);

        for conj in conjuncts {
            let tables = Self::collect_expr_tables(&conj);
            let refs_left = tables.iter().any(|t| left_tables.contains(t));
            let refs_right = tables.iter().any(|t| right_tables.contains(t));

            match (refs_left, refs_right) {
                (true, false) => left_preds.push(conj),
                (false, true) => right_preds.push(conj),
                _ => join_preds.push(conj),
            }
        }

        if left_preds.is_empty() && right_preds.is_empty() && join_preds.len() <= 1 {
            return None;
        }

        Some((
            Self::combine_and(left_preds),
            Self::combine_and(right_preds),
            Self::combine_and(join_preds),
        ))
    }

    fn flatten_and(expr: &Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                op: BinaryOperator::And,
                left,
                right,
            } => {
                let mut result = Self::flatten_and(left);
                result.extend(Self::flatten_and(right));
                result
            }
            other => vec![other.clone()],
        }
    }

    fn combine_and(exprs: Vec<Expr>) -> Option<Expr> {
        exprs.into_iter().reduce(|a, b| Expr::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(a),
            right: Box::new(b),
        })
    }

    fn collect_tables(plan: &LogicalPlan) -> Vec<String> {
        match plan {
            LogicalPlan::Scan { table, .. } => vec![table.clone()],
            LogicalPlan::Filter { input, .. } => Self::collect_tables(input),
            LogicalPlan::Project { input, .. } => Self::collect_tables(input),
            LogicalPlan::Join { left, right, .. } => {
                let mut tables = Self::collect_tables(left);
                tables.extend(Self::collect_tables(right));
                tables
            }
            _ => vec![],
        }
    }

    fn collect_expr_tables(expr: &Expr) -> Vec<String> {
        match expr {
            Expr::Column(name) => {
                if let Some(table) = name.split('.').next() {
                    if name.contains('.') {
                        return vec![table.to_string()];
                    }
                }
                vec![]
            }
            Expr::BinaryOp { left, right, .. } => {
                let mut tables = Self::collect_expr_tables(left);
                tables.extend(Self::collect_expr_tables(right));
                tables
            }
            _ => vec![],
        }
    }
}

impl Default for CostBasedOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SqlParser;

    #[test]
    fn test_predicate_pushdown_to_scan() {
        let plan = SqlParser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        let optimizer = CostBasedOptimizer::new();
        let physical = optimizer.optimize(plan);

        match physical {
            PhysicalPlan::SeqScan { filter, .. } => {
                assert!(filter.is_some());
            }
            _ => panic!("Expected SeqScan with pushed-down filter"),
        }
    }

    #[test]
    fn test_hash_join_for_equi_join() {
        let plan = SqlParser::parse(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        let optimizer = CostBasedOptimizer::new();
        let physical = optimizer.optimize(plan);

        match physical {
            PhysicalPlan::HashJoin { join_type, .. } => {
                assert_eq!(join_type, JoinType::Inner);
            }
            _ => panic!("Expected HashJoin, got {:?}", physical),
        }
    }

    #[test]
    fn test_cross_join_uses_nested_loop() {
        let plan = SqlParser::parse("SELECT * FROM a, b").unwrap();
        let optimizer = CostBasedOptimizer::new();
        let physical = optimizer.optimize(plan);

        assert!(matches!(physical, PhysicalPlan::NestedLoopJoin { .. }));
    }

    #[test]
    fn test_aggregate_uses_hash() {
        let plan =
            SqlParser::parse("SELECT department, COUNT(*) FROM emp GROUP BY department").unwrap();
        let optimizer = CostBasedOptimizer::new();
        let physical = optimizer.optimize(plan);

        match physical {
            PhysicalPlan::Project { input, .. } => {
                assert!(matches!(*input, PhysicalPlan::HashAggregate { .. }));
            }
            _ => panic!("Expected Project over HashAggregate"),
        }
    }

    #[test]
    fn test_dml_passthrough() {
        let plan = SqlParser::parse("INSERT INTO t (a) VALUES (1)").unwrap();
        let optimizer = CostBasedOptimizer::new();
        let physical = optimizer.optimize(plan);
        assert!(matches!(physical, PhysicalPlan::Insert { .. }));
    }
}
