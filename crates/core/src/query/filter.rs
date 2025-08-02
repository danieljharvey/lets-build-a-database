use super::QueryError;
use crate::types::Row;
use crate::types::Schema;
use crate::types::{Expr, Op};

#[derive(Debug)]
pub enum FilterError {
    ExpectedInt { value: serde_json::Value },
}

pub fn apply_predicate(row: &Row, schema: &Schema, where_expr: &Expr) -> Result<bool, QueryError> {
    match where_expr {
        Expr::ColumnComparison {
            column,
            op,
            literal,
        } => {
            let value = row.get_column(column, schema).ok_or_else(|| {
                QueryError::ColumnNotFoundInSchema {
                    column_name: column.clone(),
                }
            })?;

            Ok(match_op(value, op, literal).map_err(QueryError::FilterError)?)
        }
    }
}

fn match_op(
    value: &serde_json::Value,
    op: &Op,
    literal: &serde_json::Value,
) -> Result<bool, FilterError> {
    match op {
        Op::Equals => Ok(value == literal),
        Op::GreaterThan => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(left > right)
        }
        Op::GreaterThanOrEqual => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(left >= right)
        }
        Op::LessThan => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(left < right)
        }
        Op::LessThanOrEqual => {
            let left = as_int(value)?;
            let right = as_int(literal)?;
            Ok(left <= right)
        }
    }
}

fn as_int(value: &serde_json::Value) -> Result<i64, FilterError> {
    value.as_i64().ok_or_else(|| FilterError::ExpectedInt {
        value: value.clone(),
    })
}
