mod filter;
mod from;
mod join;
mod order_by;
mod project;

use crate::types::{Limit, OrderBy};

use super::types::QueryStep;
use super::types::{Column, Filter, From, Join, Project, Query};

#[derive(Debug)]
pub enum QueryError {
    ColumnNotFoundInSchema { column_name: Column },
    IndexNotFoundInSchema { index: usize },
    FilterError(filter::FilterError),
}

pub fn run_query(query: &Query) -> Result<QueryStep, QueryError> {
    match query {
        Query::From(From {
            table_name,
            table_alias,
        }) => Ok(from::table_scan(table_name, table_alias.as_ref())),
        Query::Filter(Filter { from, filter }) => {
            let QueryStep {
                schema,
                rows,
                mut cost,
            } = run_query(from)?;

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
        Query::Project(Project { from, fields }) => {
            let QueryStep {
                schema,
                rows,
                mut cost,
            } = run_query(from)?;

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
        Query::Limit(Limit { limit, from }) => {
            let QueryStep {
                schema,
                mut rows,
                cost,
            } = run_query(from)?;
            let size: usize = (*limit).try_into().unwrap();

            rows.truncate(size);

            Ok(QueryStep { schema, rows, cost })
        }
        Query::Join(Join {
            left_from,
            right_from,
            join_type,
            on,
        }) => {
            let QueryStep {
                schema: left_schema,
                rows: left_rows,
                cost: mut left_cost,
            } = run_query(left_from)?;

            let QueryStep {
                schema: right_schema,
                rows: right_rows,
                cost: right_cost,
            } = run_query(right_from)?;

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
        Query::OrderBy(OrderBy {
            from,
            order_by_exprs,
        }) => {
            let QueryStep {
                schema,
                rows,
                mut cost,
            } = run_query(from)?;

            let rows = order_by::order_by(rows, &schema, order_by_exprs, &mut cost);

            Ok(QueryStep { schema, rows, cost })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{parser::parse, run_query};

    #[test]
    fn test_query_select_animals() {
        let query = parse("SELECT * FROM animal").unwrap();

        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_query_select_horse() {
        let query = parse("select * from animal where animal_name = 'horse'").unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_query_projection() {
        let query = parse("select animal_name from animal where animal_name = 'horse'").unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_horse_and_species() {
        let query = parse(
            r"
        select * from animal 
        join species 
            on species_id 
        where animal_name = 'horse'",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_species_and_animals() {
        let query = parse(
            r"
        select * from species 
          join animal on species_id
        where
          species_id >= 3 
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_species_and_animals_left_outer() {
        let query = parse(
            r"
        select * from species 
          left outer join animal on species_id
        where
          species_id = 3 
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_album() {
        let query = parse(
            r"
        select * from Album 
        where Title = 'Jagged Little Pill'
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_album_and_artist() {
        let query = parse(
            r"
        select * from Album 
          join Artist on ArtistId
        where
          ArtistId = 82
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_track_album_and_artist() {
        let query = parse(
            r"
        select Name, Title, artist.Name from Track
          join Album on AlbumId
          join Artist as artist on ArtistId
        where
          ArtistId = 82
        limit 10
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_filter_with_column_reference() {
        let query = parse(
            r"
        select * from Album 
        where AlbumId = (ArtistId + 1 + 1) - 1 
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_order_by_name_limit_5() {
        let query = parse(
            r"
        select * from Album
        order by Title
        limit 5
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }

    #[test]
    fn test_select_order_by_artist_id_and_title_with_limit() {
        let query = parse(
            r"
        select ArtistId, Title from Album
        order by ArtistId, Title
        limit 4
    ",
        )
        .unwrap();
        let result = run_query(&query).unwrap();

        insta::assert_json_snapshot!(result.to_json());
        insta::assert_debug_snapshot!(result.cost);
    }
}
