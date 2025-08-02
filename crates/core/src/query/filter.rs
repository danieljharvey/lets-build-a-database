use super::QueryError;
use crate::types::Row;
use crate::types::Schema;
use crate::types::{Expr, Op};

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

            Ok(match op {
                Op::Equals => value == literal,
            })
        }
    }
}
