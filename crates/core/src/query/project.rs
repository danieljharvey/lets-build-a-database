use super::QueryError;
use crate::types::Column;
use crate::types::Row;
use crate::types::Schema;

pub fn project_schema(schema: &Schema, fields: &[Column]) -> Result<Schema, QueryError> {
    let mut columns = vec![];

    for field in fields {
        let index = schema.get_index_for_column(field).ok_or_else(|| {
            QueryError::ColumnNotFoundInSchema {
                column_name: field.clone(),
            }
        })?;

        let column = schema
            .columns
            .get(index)
            .ok_or(QueryError::IndexNotFoundInSchema { index })?;

        columns.push(column.clone());
    }

    Ok(Schema { columns })
}

// filter columns out of a row
pub fn project_fields(row: &Row, schema: &Schema, fields: &[Column]) -> Result<Row, QueryError> {
    let mut items = vec![];

    for field in fields {
        let item =
            row.get_column(field, schema)
                .ok_or_else(|| QueryError::ColumnNotFoundInSchema {
                    column_name: field.clone(),
                })?;

        items.push(item.clone());
    }

    Ok(Row { items })
}
