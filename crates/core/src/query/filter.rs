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
    match evaluate_expr(row, None, schema, where_expr)? {
        serde_json::Value::Bool(b) => Ok(b),
        other => Err(QueryError::FilterError(FilterError::ExpectedBooleanType {
            value: other,
        })),
    }
}

pub fn evaluate_expr(
    row: &Row,
    all_rows: Option<&Vec<Row>>,
    schema: &Schema,
    expr: &Expr,
) -> Result<serde_json::Value, QueryError> {
    match expr {
        Expr::BinaryOperation { left, op, right } => {
            let left = evaluate_expr(row, all_rows, schema, left)?;
            let right = evaluate_expr(row, all_rows, schema, right)?;

            match_op(&left, op, &right).map_err(QueryError::FilterError)
        }
        Expr::Column { column } => row
            .get_column(column, schema)
            .ok_or_else(|| QueryError::ColumnNotFoundInSchema {
                column_name: column.clone(),
            })
            .cloned(),
        Expr::Literal { literal } => Ok(literal.clone()),
        Expr::Nested { expr } => evaluate_expr(row, all_rows, schema, expr),
        Expr::FunctionCall {
            function_name,
            args,
        } => evaluate_function_call(function_name, args, row, all_rows, schema),
    }
}

fn evaluate_function_call(
    function_name: &FunctionName,
    args: &Vec<Expr>,
    row: &Row,
    all_rows: Option<&Vec<Row>>,
    schema: &Schema,
) -> Result<serde_json::Value, QueryError> {
    match function_name {
        FunctionName::Aggregate(agg) => match agg {
            AggregateFunctionName::Sum => {
                let expr = args.first().ok_or_else(|| QueryError::ArgumentNotFound)?;

                let Some(all_rows) = all_rows else {
                    return Err(QueryError::CannotUseAggregateFunctionInFilter);
                };

                let sum = all_rows.iter().try_fold(0, |total, all_row| {
                    let value = evaluate_expr(all_row, None, schema, expr)?;

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
