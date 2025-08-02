use super::QueryError;
use crate::types::QueryStep;
use crate::types::Row;
use crate::types::Schema;
use crate::types::{Column, JoinType};
use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

pub fn hash_join(
    left_rows: Vec<Row>,
    left_schema: &Schema,
    right_rows: Vec<Row>,
    right_schema: &Schema,
    left_on: &Column,
    right_on: &Column,
    join_type: &JoinType,
) -> Result<QueryStep, QueryError> {
    let mut stuff = HashMap::new();

    // add all the relevent `on` values to map,
    for left_row in &left_rows {
        let value = left_row.get_column(left_on, left_schema).ok_or_else(|| {
            QueryError::ColumnNotFoundInSchema {
                column_name: left_on.clone(),
            }
        })?;

        stuff.insert(calculate_hash(value), vec![]);
    }

    // collect all the different right side values
    for right_row in right_rows {
        let value = right_row
            .get_column(right_on, right_schema)
            .ok_or_else(|| QueryError::ColumnNotFoundInSchema {
                column_name: right_on.clone(),
            })?;

        // this assumes left join and ignores where there's no left match
        if let Some(items) = stuff.get_mut(&calculate_hash(value)) {
            items.push(right_row.clone());
        }
    }

    let mut output_rows = vec![];

    for left_row in left_rows {
        let hash = calculate_hash(left_row.get_column(left_on, left_schema).ok_or_else(|| {
            QueryError::ColumnNotFoundInSchema {
                column_name: left_on.clone(),
            }
        })?);

        if let Some(rhs) = stuff.get(&hash) {
            if rhs.is_empty() {
                // if left outer join
                if let JoinType::LeftOuter = join_type {
                    let mut whole_row = left_row.clone();

                    // we can't find value, so add a bunch of nulls
                    for _ in &right_schema.columns {
                        whole_row.items.push(serde_json::Value::Null);
                    }
                    output_rows.push(whole_row);
                }
            } else {
                for item in rhs {
                    let mut whole_row = left_row.clone();
                    whole_row.extend(item.clone());
                    output_rows.push(whole_row);
                }
            }
        }
    }

    let mut schema = left_schema.clone();
    schema.extend(right_schema.clone());

    Ok(QueryStep {
        rows: output_rows,
        schema,
    })
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
