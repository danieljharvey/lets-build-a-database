use std::cmp::Ordering;

use crate::types::{Cost, Order, OrderByExpr, Row, Schema};

pub fn order_by(
    mut rows: Vec<Row>,
    schema: &Schema,
    order_by_exprs: &[OrderByExpr],
    cost: &mut Cost,
) -> Vec<Row> {
    rows.sort_by(|row_a, row_b| {
        cost.increment_rows_processed();
        order_by_exprs
            .iter()
            .fold(Ordering::Equal, |ordering, order_by_expr| {
                // if the ordering is still unknown
                if ordering == Ordering::Equal {
                    let a = row_a.get_column(&order_by_expr.column, schema).unwrap();
                    let b = row_b.get_column(&order_by_expr.column, schema).unwrap();

                    let ordering = compare_values(a, b);

                    match order_by_expr.order {
                        Order::Asc => ordering,
                        Order::Desc => flip(ordering),
                    }
                } else {
                    // stick with the ordering we have
                    ordering
                }
            })
    });
    rows
}

fn flip(ordering: Ordering) -> Ordering {
    match ordering {
        Ordering::Equal => Ordering::Equal,
        Ordering::Less => Ordering::Greater,
        Ordering::Greater => Ordering::Less,
    }
}

fn compare_values(a: &serde_json::Value, b: &serde_json::Value) -> Ordering {
    match (a, b) {
        (serde_json::Value::Null, serde_json::Value::Null) => Ordering::Equal,
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a.cmp(b),
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            if let Some((a, b)) = a.as_i64().zip(b.as_i64()) {
                a.cmp(&b)
            } else if let Some((a, b)) = a.as_f64().zip(b.as_f64()) {
                a.total_cmp(&b)
            } else {
                Ordering::Equal
            }
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a.cmp(b),
        _ => todo!("Unsupported ordering"),
    }
}
