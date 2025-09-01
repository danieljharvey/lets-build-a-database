use super::QueryError;
use crate::types::AggregateFunctionName;
use crate::types::FunctionName;
use crate::types::Row;
use crate::types::Schema;
use crate::types::{Expr, Op};

#[derive(Debug)]
pub enum FilterError {
    ExpectedInt { value: serde_json::Value },
    ExpectedBooleanType { value: serde_json::Value },
}

pub fn apply_predicate(row: &Row, schema: &Schema, where_expr: &Expr) -> Result<bool, QueryError> {
    match evaluate_expr(row, schema, where_expr)? {
        serde_json::Value::Bool(b) => Ok(b),
        other => Err(QueryError::FilterError(FilterError::ExpectedBooleanType {
            value: other,
        })),
    }
}

pub fn evaluate_expr(
    row: &Row,
    schema: &Schema,
    expr: &Expr,
) -> Result<serde_json::Value, QueryError> {
    match expr {
        Expr::BinaryOperation { left, op, right } => {
            let left = evaluate_expr(row, schema, left)?;
            let right = evaluate_expr(row, schema, right)?;

            match_op(&left, op, &right).map_err(QueryError::FilterError)
        }
        Expr::Column { column } => row
            .get_column(column, schema)
            .ok_or_else(|| QueryError::ColumnNotFoundInSchema {
                column_name: column.clone(),
            })
            .cloned(),
        Expr::Literal { literal } => Ok(literal.clone()),
        Expr::Nested { expr } => evaluate_expr(row, schema, expr),
        Expr::FunctionCall { .. } => todo!("function call in evaluate_expr"),
    }
}

pub fn evaluate_aggregate_expr(
    all_rows: &Vec<Row>,
    schema: &Schema,
    expr: &Expr,
) -> Result<serde_json::Value, QueryError> {
    match expr {
        Expr::BinaryOperation { left, op, right } => {
            let left = evaluate_aggregate_expr(all_rows, schema, left)?;
            let right = evaluate_aggregate_expr(all_rows, schema, right)?;

            match_op(&left, op, &right).map_err(QueryError::FilterError)
        }
        Expr::Column { .. } => panic!("column in evaluate_aggregate_expr"),
        Expr::Literal { literal } => Ok(literal.clone()),
        Expr::Nested { expr } => evaluate_aggregate_expr(all_rows, schema, expr),
        Expr::FunctionCall {
            function_name,
            args,
        } => evaluate_function_call(function_name, args, all_rows, schema),
    }
}

fn evaluate_function_call(
    function_name: &FunctionName,
    args: &Vec<Expr>,
    all_rows: &Vec<Row>,
    schema: &Schema,
) -> Result<serde_json::Value, QueryError> {
    match function_name {
        FunctionName::Aggregate(agg) => match agg {
            AggregateFunctionName::Sum => {
                let expr = args.first().ok_or_else(|| QueryError::ArgumentNotFound)?;

                let sum = all_rows.iter().try_fold(0, |total, all_row| {
                    let value = evaluate_expr(all_row, schema, expr)?;

                    if let serde_json::Value::Number(a) = value {
                        if let Some(a) = a.as_i64() {
                            return Ok(total + a);
                        }
                    };

                    Err(QueryError::TypeMismatch {
                        expected: "i64".into(),
                    })
                })?;

                Ok(sum.into())
            }
        },
    }
}

fn match_op(
    value: &serde_json::Value,
    op: &Op,
    literal: &serde_json::Value,
) -> Result<serde_json::Value, FilterError> {
    match op {
        Op::Equals => Ok(serde_json::Value::Bool(value == literal)),
        Op::GreaterThan => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(serde_json::Value::Bool(left > right))
        }
        Op::GreaterThanOrEqual => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(serde_json::Value::Bool(left >= right))
        }
        Op::LessThan => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(serde_json::Value::Bool(left < right))
        }
        Op::LessThanOrEqual => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(serde_json::Value::Bool(left <= right))
        }
        Op::Add => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(serde_json::Value::Number((left + right).into()))
        }
        Op::Subtract => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(serde_json::Value::Number((left - right).into()))
        }
    }
}

fn as_int(value: &serde_json::Value) -> Result<i64, FilterError> {
    value.as_i64().ok_or_else(|| FilterError::ExpectedInt {
        value: value.clone(),
    })
}
