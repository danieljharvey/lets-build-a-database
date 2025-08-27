use super::filter::evaluate_expr;
use super::QueryError;
use crate::types::Cost;
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
            _ => todo!("project schema"),
        }
    }

    Ok(Schema { columns })
}

pub fn project_fields(
    rows: &Vec<Row>,
    schema: &Schema,
    fields: &[Expr],
    cost: &mut Cost,
) -> Result<Vec<Row>, QueryError> {
    let mut projected_rows = vec![];

    for row in rows {
        cost.increment_rows_processed();
        projected_rows.push(project_field_row(row, rows, &schema, fields)?);
    }
    Ok(projected_rows)
}

// filter columns out of a row
pub fn project_field_row(
    row: &Row,
    all_rows: &Vec<Row>,
    schema: &Schema,
    fields: &[Expr],
) -> Result<Row, QueryError> {
    let mut items = vec![];

    for field in fields {
        let item = evaluate_expr(row, Some(all_rows), schema, field)?;

        items.push(item.clone());
    }

    Ok(Row { items })
}
