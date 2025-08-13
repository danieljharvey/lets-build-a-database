mod filter;
mod from;
mod join;
mod project;
mod to_physical_plan;

use std::collections::BTreeMap;

use crate::types::{IndexScan, Limit, PhysicalPlan, TableScan};
use to_physical_plan::to_physical_plan;

use super::types::QueryStep;
use super::types::{Column, Filter, Join, LogicalPlan, Project};

#[derive(Debug)]
pub enum QueryError {
    ColumnNotFoundInSchema { column_name: Column },
    IndexNotFoundInSchema { index: usize },
    FilterError(filter::FilterError),
}

fn run_physical_plan(physical_plan: &PhysicalPlan) -> Result<QueryStep, QueryError> {
    match physical_plan {
        PhysicalPlan::TableScan(TableScan {
            table_name,
            table_alias,
        }) => Ok(from::table_scan(table_name, table_alias.as_ref())),
        PhysicalPlan::IndexScan(IndexScan { .. }) => todo!("index scan"),
        PhysicalPlan::Filter(Filter { from, filter }) => {
            let QueryStep {
                schema,
                rows,
                mut cost,
            } = run_physical_plan(from)?;

            let mut filtered_rows = vec![];

            for row in rows {
                cost.increment_rows_processed();
                if filter::apply_predicate(&row, &schema, filter)? {
                    filtered_rows.push(row);
                }
            }

            Ok(QueryStep {
                schema,
                rows: filtered_rows,
                cost,
            })
        }
        PhysicalPlan::Project(Project { from, fields }) => {
            let QueryStep {
                schema,
                rows,
                mut cost,
            } = run_physical_plan(from)?;

            let mut projected_rows = vec![];

            for row in &rows {
                cost.increment_rows_processed();
                projected_rows.push(project::project_fields(row, &schema, fields)?);
            }

            let schema = project::project_schema(&schema, fields)?;

            Ok(QueryStep {
                schema,
                rows: projected_rows,
                cost,
            })
        }
        PhysicalPlan::Limit(Limit { limit, from }) => {
            let QueryStep {
                schema,
                mut rows,
                cost,
            } = run_physical_plan(from)?;
            let size: usize = (*limit).try_into().unwrap();

            rows.truncate(size);

            Ok(QueryStep { schema, rows, cost })
        }
        PhysicalPlan::Join(Join {
            left_from,
            right_from,
            join_type,
            on,
        }) => {
            let QueryStep {
                schema: left_schema,
                rows: left_rows,
                cost: mut left_cost,
            } = run_physical_plan(left_from)?;

            let QueryStep {
                schema: right_schema,
                rows: right_rows,
                cost: right_cost,
            } = run_physical_plan(right_from)?;

            left_cost.extend(&right_cost);

            join::hash_join(
                left_rows,
                &left_schema,
                right_rows,
                &right_schema,
                on,
                join_type,
                left_cost,
            )
        }
    }
}

// TODO: pre-calculate indexes and pass them in
pub fn run_query(query: LogicalPlan) -> Result<QueryStep, QueryError> {
    let physical_plan = to_physical_plan(query, &BTreeMap::new());
    run_physical_plan(&physical_plan)
}

#[cfg(test)]
mod tests {
    use crate::{parser::parse, run_query};

    #[test]
    fn test_query_select_animals() {
        let query = parse("SELECT * FROM animal").unwrap();

        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_query_select_horse() {
        let query = parse("select * from animal where animal_name = 'horse'").unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_query_projection() {
        let query = parse("select animal_name from animal where animal_name = 'horse'").unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_horse_and_species() {
        let query = parse(
            r#"
        select * from animal 
        join species 
            on species_id 
        where animal_name = 'horse'"#,
        )
        .unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_species_and_animals() {
        let query = parse(
            r#"
        select * from species 
          join animal on species_id
        where
          species_id >= 3 
    "#,
        )
        .unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_species_and_animals_left_outer() {
        let query = parse(
            r#"
        select * from species 
          left outer join animal on species_id
        where
          species_id = 3 
    "#,
        )
        .unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_album() {
        let query = parse(
            r#"
        select * from Album 
        where Title = 'Jagged Little Pill'
    "#,
        )
        .unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_album_and_artist() {
        let query = parse(
            r#"
        select * from Album 
          join Artist on ArtistId
        where
          ArtistId = 82
    "#,
        )
        .unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_track_album_and_artist() {
        let query = parse(
            r#"
        select Name, Title, artist.Name from Track
          join Album on AlbumId
          join Artist as artist on ArtistId
        where
          ArtistId = 82
        limit 10
    "#,
        )
        .unwrap();
        let result = run_query(query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }
}
