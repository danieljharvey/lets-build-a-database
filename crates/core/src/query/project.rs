use std::collections::BTreeMap;

use super::filter::evaluate_aggregate_expr;
use super::filter::evaluate_expr;
use super::QueryError;
use crate::types::Cost;
use crate::types::Expr;
use crate::types::FunctionName;
use crate::types::Row;
use crate::types::Schema;
use crate::types::SchemaColumn;

pub fn project_schema(schema: &Schema, fields: &[Expr]) -> Result<Schema, QueryError> {
    let mut columns = vec![];

    for field in fields {
        let schema_column = index_for_expr(field, schema)?;

        columns.push(schema_column);
    }

    Ok(Schema { columns })
}

fn index_for_expr(field: &Expr, schema: &Schema) -> Result<SchemaColumn, QueryError> {
    match field {
        Expr::Column { column } => {
            let index = schema.get_index_for_column(column).ok_or_else(|| {
                QueryError::ColumnNotFoundInSchema {
                    column_name: column.clone(),
                }
            })?;
            schema
                .columns
                .get(index)
                .ok_or(QueryError::IndexNotFoundInSchema { index })
                .cloned()
        }
        Expr::Literal { literal } => {
            let name = format!("{literal}");
            Ok(SchemaColumn::Named(name))
        }
        Expr::BinaryOperation { op, .. } => {
            let name = format!("{op}");

            Ok(SchemaColumn::Named(name))
        }
        Expr::Nested { expr } => index_for_expr(expr, schema),
        Expr::FunctionCall { function_name, .. } => {
            let name = format!("{function_name}");
            Ok(SchemaColumn::Named(name))
        }
    }
}

fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } => false,
        Expr::Literal { .. } => false,
        Expr::BinaryOperation { left, right, .. } => {
            is_aggregate_expr(left) || is_aggregate_expr(right)
        }
        Expr::Nested { expr } => is_aggregate_expr(expr),
        Expr::FunctionCall {
            function_name,
            args,
        } => {
            let is_aggregate_function = match function_name {
                FunctionName::Aggregate(_) => true,
            };
            is_aggregate_function || args.iter().any(is_aggregate_expr)
        }
    }
}

pub fn project_fields(
    rows: &Vec<Row>,
    schema: &Schema,
    fields: &[Expr],
    cost: &mut Cost,
) -> Result<Vec<Row>, QueryError> {
    let mut aggregate_results = BTreeMap::new();

    // first work out the values for aggregate fields
    for (index, field) in fields.iter().enumerate() {
        if is_aggregate_expr(field) {
            aggregate_results.insert(index, evaluate_aggregate_expr(rows, schema, field)?);
        }
    }

    // if we only have aggregate fields, we only return one row of totals
    // otherwise we return one for each row with the aggreagtes mixed in
    if aggregate_results.len() == fields.len() {
        let mut items = vec![];
        for (index, _) in fields.iter().enumerate() {
            let value = aggregate_results.get(&index).unwrap();
            items.push(value.clone());
        }
        Ok(vec![Row { items }])
    } else {
        let mut projected_rows = vec![];

        // then add regular fields
        for row in rows {
            cost.increment_rows_processed();
            projected_rows.push(project_field_row(row, &schema, fields, &aggregate_results)?);
        }
        Ok(projected_rows)
    }
}

// filter columns out of a row
pub fn project_field_row(
    row: &Row,
    schema: &Schema,
    fields: &[Expr],
    aggregate_results: &BTreeMap<usize, serde_json::Value>,
) -> Result<Row, QueryError> {
    let mut items = vec![];

    // use aggregate value for field, or calculate for this particular row
    for (index, field) in fields.iter().enumerate() {
        let value = if let Some(agg_value) = aggregate_results.get(&index) {
            agg_value.clone()
        } else {
            evaluate_expr(row, schema, field)?
        };

        items.push(value.clone());
    }

    Ok(Row { items })
}
