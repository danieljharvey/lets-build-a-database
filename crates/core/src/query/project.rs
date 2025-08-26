use super::QueryError;
use crate::types::Expr;
use crate::types::Row;
use crate::types::Schema;

pub fn project_schema(schema: &Schema, fields: &[Expr]) -> Result<Schema, QueryError> {
    let mut columns = vec![];

    for field in fields {
        match field {
            Expr::Column { column } => {
                let index = schema.get_index_for_column(column).ok_or_else(|| {
                    QueryError::ColumnNotFoundInSchema {
                        column_name: column.clone(),
                    }
                })?;

                let column = schema
                    .columns
                    .get(index)
                    .ok_or(QueryError::IndexNotFoundInSchema { index })?;

                columns.push(column.clone());
            }
            _ => todo!("can't project schema"),
        }
    }

    Ok(Schema { columns })
}

// filter columns out of a row
pub fn project_fields(row: &Row, schema: &Schema, fields: &[Expr]) -> Result<Row, QueryError> {
    let mut items = vec![];

    for field in fields {
        match field {
            Expr::Column { column } => {
                let item = row.get_column(column, schema).ok_or_else(|| {
                    QueryError::ColumnNotFoundInSchema {
                        column_name: column.clone(),
                    }
                })?;

                items.push(item.clone());
            }
            _ => todo!("can't project field"),
        }
    }

    Ok(Row { items })
}
