use crate::executor::Row;
#[cfg(test)]
use crate::expr::BinaryOperator;
use crate::expr::{Expr, Value};
use crate::plan::JoinType;
use std::collections::HashMap;

pub struct NestedLoopJoin;

impl NestedLoopJoin {
    pub fn execute(
        left: &[Row],
        right: &[Row],
        join_type: JoinType,
        condition: Option<&Expr>,
        eval: &dyn Fn(&Expr, &Row) -> Option<Value>,
    ) -> Vec<Row> {
        let mut results = Vec::new();

        match join_type {
            JoinType::Inner => {
                for l in left {
                    for r in right {
                        let merged = merge_rows(l, r);
                        if matches_condition(&merged, condition, eval) {
                            results.push(merged);
                        }
                    }
                }
            }
            JoinType::Left => {
                for l in left {
                    let mut matched = false;
                    for r in right {
                        let merged = merge_rows(l, r);
                        if matches_condition(&merged, condition, eval) {
                            results.push(merged);
                            matched = true;
                        }
                    }
                    if !matched {
                        results.push(l.clone());
                    }
                }
            }
            JoinType::Right => {
                for r in right {
                    let mut matched = false;
                    for l in left {
                        let merged = merge_rows(l, r);
                        if matches_condition(&merged, condition, eval) {
                            results.push(merged);
                            matched = true;
                        }
                    }
                    if !matched {
                        results.push(r.clone());
                    }
                }
            }
            JoinType::Cross => {
                for l in left {
                    for r in right {
                        results.push(merge_rows(l, r));
                    }
                }
            }
        }

        results
    }
}

pub struct HashJoin;

impl HashJoin {
    pub fn execute(
        left: &[Row],
        right: &[Row],
        join_type: JoinType,
        left_key: &Expr,
        right_key: &Expr,
        eval: &dyn Fn(&Expr, &Row) -> Option<Value>,
    ) -> Vec<Row> {
        let mut hash_table: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();

        for (i, row) in left.iter().enumerate() {
            if let Some(key_val) = eval(left_key, row) {
                let key = key_val.to_sort_key();
                hash_table.entry(key).or_default().push(i);
            }
        }

        let mut results = Vec::new();
        let mut left_matched = vec![false; left.len()];

        for r in right {
            if let Some(key_val) = eval(right_key, r) {
                let key = key_val.to_sort_key();
                if let Some(indices) = hash_table.get(&key) {
                    for &i in indices {
                        let merged = merge_rows(&left[i], r);
                        results.push(merged);
                        left_matched[i] = true;
                    }
                } else if join_type == JoinType::Right {
                    results.push(r.clone());
                }
            }
        }

        if join_type == JoinType::Left {
            for (i, matched) in left_matched.iter().enumerate() {
                if !matched {
                    results.push(left[i].clone());
                }
            }
        }

        results
    }
}

pub struct SortMergeJoin;

impl SortMergeJoin {
    pub fn execute(
        left: &[Row],
        right: &[Row],
        join_type: JoinType,
        left_key: &Expr,
        right_key: &Expr,
        eval: &dyn Fn(&Expr, &Row) -> Option<Value>,
    ) -> Vec<Row> {
        let mut left_sorted: Vec<(Vec<u8>, usize)> = left
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                eval(left_key, row).map(|v| (v.to_sort_key(), i))
            })
            .collect();
        let mut right_sorted: Vec<(Vec<u8>, usize)> = right
            .iter()
            .enumerate()
            .filter_map(|(i, row)| {
                eval(right_key, row).map(|v| (v.to_sort_key(), i))
            })
            .collect();

        left_sorted.sort_by(|a, b| a.0.cmp(&b.0));
        right_sorted.sort_by(|a, b| a.0.cmp(&b.0));

        let mut results = Vec::new();
        let mut left_matched = vec![false; left.len()];
        let mut right_matched = vec![false; right.len()];
        let mut li = 0;
        let mut ri = 0;

        while li < left_sorted.len() && ri < right_sorted.len() {
            match left_sorted[li].0.cmp(&right_sorted[ri].0) {
                std::cmp::Ordering::Less => li += 1,
                std::cmp::Ordering::Greater => ri += 1,
                std::cmp::Ordering::Equal => {
                    let key = &left_sorted[li].0;
                    let li_start = li;
                    let ri_start = ri;

                    while li < left_sorted.len() && left_sorted[li].0 == *key {
                        li += 1;
                    }
                    while ri < right_sorted.len() && right_sorted[ri].0 == *key {
                        ri += 1;
                    }

                    for &(_, left_idx) in &left_sorted[li_start..li] {
                        for &(_, right_idx) in &right_sorted[ri_start..ri] {
                            results.push(merge_rows(&left[left_idx], &right[right_idx]));
                            left_matched[left_idx] = true;
                            right_matched[right_idx] = true;
                        }
                    }
                }
            }
        }

        match join_type {
            JoinType::Left => {
                for (i, matched) in left_matched.iter().enumerate() {
                    if !matched {
                        results.push(left[i].clone());
                    }
                }
            }
            JoinType::Right => {
                for (i, matched) in right_matched.iter().enumerate() {
                    if !matched {
                        results.push(right[i].clone());
                    }
                }
            }
            _ => {}
        }

        results
    }
}

fn merge_rows(left: &Row, right: &Row) -> Row {
    let mut columns = left.columns_map().clone();
    for (k, v) in right.columns_map() {
        columns.insert(k.clone(), v.clone());
    }
    Row::from_map(columns)
}

fn matches_condition(
    row: &Row,
    condition: Option<&Expr>,
    eval: &dyn Fn(&Expr, &Row) -> Option<Value>,
) -> bool {
    match condition {
        None => true,
        Some(expr) => eval(expr, row)
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(pairs: Vec<(&str, Value)>) -> Row {
        Row::new_with_values(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    fn simple_eval(expr: &Expr, row: &Row) -> Option<Value> {
        match expr {
            Expr::Column(name) => row.get_column(name),
            Expr::Literal(v) => Some(v.clone()),
            Expr::BinaryOp { op, left, right } => {
                let l = simple_eval(left, row)?;
                let r = simple_eval(right, row)?;
                match op {
                    BinaryOperator::Eq => Some(Value::Bool(l == r)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    #[test]
    fn test_hash_join_inner() {
        let left = vec![
            make_row(vec![("id", Value::Int(1)), ("name", Value::String("Alice".into()))]),
            make_row(vec![("id", Value::Int(2)), ("name", Value::String("Bob".into()))]),
            make_row(vec![("id", Value::Int(3)), ("name", Value::String("Charlie".into()))]),
        ];
        let right = vec![
            make_row(vec![("user_id", Value::Int(1)), ("amount", Value::Int(100))]),
            make_row(vec![("user_id", Value::Int(2)), ("amount", Value::Int(200))]),
            make_row(vec![("user_id", Value::Int(4)), ("amount", Value::Int(400))]),
        ];

        let left_key = Expr::Column("id".into());
        let right_key = Expr::Column("user_id".into());

        let result = HashJoin::execute(&left, &right, JoinType::Inner, &left_key, &right_key, &simple_eval);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_hash_join_left() {
        let left = vec![
            make_row(vec![("id", Value::Int(1))]),
            make_row(vec![("id", Value::Int(2))]),
        ];
        let right = vec![make_row(vec![("id", Value::Int(1))])];

        let result = HashJoin::execute(
            &left,
            &right,
            JoinType::Left,
            &Expr::Column("id".into()),
            &Expr::Column("id".into()),
            &simple_eval,
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_sort_merge_join() {
        let left = vec![
            make_row(vec![("a", Value::Int(1))]),
            make_row(vec![("a", Value::Int(2))]),
            make_row(vec![("a", Value::Int(3))]),
        ];
        let right = vec![
            make_row(vec![("b", Value::Int(2))]),
            make_row(vec![("b", Value::Int(3))]),
            make_row(vec![("b", Value::Int(4))]),
        ];

        let result = SortMergeJoin::execute(
            &left,
            &right,
            JoinType::Inner,
            &Expr::Column("a".into()),
            &Expr::Column("b".into()),
            &simple_eval,
        );
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_nested_loop_cross() {
        let left = vec![
            make_row(vec![("x", Value::Int(1))]),
            make_row(vec![("x", Value::Int(2))]),
        ];
        let right = vec![
            make_row(vec![("y", Value::Int(10))]),
            make_row(vec![("y", Value::Int(20))]),
        ];

        let result = NestedLoopJoin::execute(&left, &right, JoinType::Cross, None, &simple_eval);
        assert_eq!(result.len(), 4);
    }
}
