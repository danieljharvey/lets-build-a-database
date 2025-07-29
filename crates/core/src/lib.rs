mod parser;
mod types;

pub use parser::parse;
use serde_json::json;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use types::QueryStep;
use types::Row;
use types::Schema;
use types::{Column, Expr, Filter, From, Join, JoinType, Op, Project, Query, TableName};

// hard coded vec of column names for now
fn schema(table_name: &TableName) -> Vec<Column> {
    match table_name.0.as_str() {
        "animal" => vec![
            "animal_id".into(),
            "animal_name".into(),
            "species_id".into(),
        ],
        "species" => vec!["species_id".into(), "species_name".into()],
        "Album" => vec!["AlbumId".into(), "Title".into(), "ArtistId".into()],
        "Artist" => vec!["ArtistId".into(), "Name".into()],
        "Track" => vec![
            "TrackId".into(),
            "Name".into(),
            "AlbumId".into(),
            "MediaTypeId".into(),
            "GenreId".into(),
            "Composer".into(),
            "Milliseconds".into(),
            "Bytes".into(),
            "UnitPrice".into(),
        ],
        _ => todo!("unknown schema"),
    }
}

// scan of static values for now
fn table_scan(table_name: &TableName) -> QueryStep {
    let columns = schema(table_name);

    let raw = match table_name.0.as_str() {
        "animal" => [(1, "horse", 1), (2, "dog", 1), (3, "snake", 2)]
            .iter()
            .map(|(id, name, species)| json!({ "animal_id": id, "animal_name": name, "species_id": species }))
            .collect(),
        "species" => [(1, "mammal"), (2, "reptile"), (3, "bird")]
            .iter()
            .map(|(id, name)| json!({"species_id": id, "species_name": name}))
            .collect(),
        "Album" => {
            let my_str = include_str!("../static/Album.json");
            serde_json::from_str::<Vec<serde_json::Value>>(my_str).unwrap()
        },
        "Artist" => {
            let my_str = include_str!("../static/Artist.json");
            serde_json::from_str::<Vec<serde_json::Value>>(my_str).unwrap()
        }
        "Track" => {
            let my_str = include_str!("../static/Track.json");
            serde_json::from_str::<Vec<serde_json::Value>>(my_str).unwrap()
        }
        _ => todo!("table not found {table_name:?}"),
    };

    let rows = raw.into_iter().map(|raw| into_row(raw, &columns)).collect();

    QueryStep {
        schema: Schema { columns },
        rows,
    }
}

fn into_row(value: serde_json::Value, columns: &Vec<Column>) -> Row {
    let serde_json::Value::Object(mut map) = value else {
        panic!("what is this")
    };

    let mut items = vec![];

    // collect items in order
    for column in columns {
        let Some(item) = map.remove(&column.name) else {
            panic!("could not find {}", column.name);
        };

        items.push(item)
    }

    Row { items }
}

pub fn run_query(query: &Query) -> QueryStep {
    match query {
        Query::From(From { table_name }) => table_scan(table_name),
        Query::Filter(Filter { from, filter }) => {
            let QueryStep { schema, rows } = run_query(from);
            let rows = rows
                .into_iter()
                .filter(|row| apply_predicate(row, &schema, filter))
                .collect();

            QueryStep { schema, rows }
        }
        Query::Project(Project { from, fields }) => {
            let QueryStep { schema, rows } = run_query(from);

            // TODO, we'll probably want to change the schema here
            let rows = rows
                .into_iter()
                .map(|row| project_fields(row, &schema, fields))
                .collect();

            QueryStep { schema, rows }
        }
        Query::Join(Join {
            left_from,
            right_from,
            left_column_on,
            right_column_on,
            join_type,
        }) => {
            let QueryStep {
                schema: left_schema,
                rows: left_rows,
            } = run_query(left_from);
            let QueryStep {
                schema: right_schema,
                rows: right_rows,
            } = run_query(right_from);

            todo!("join");
            /*
            hash_join(
                left_rows,
                right_rows,
                left_column_on,
                right_column_on,
                join_type,
            )*/
        }
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn hash_join(
    left_rows: Vec<serde_json::Value>,
    right_rows: Vec<serde_json::Value>,
    left_on: &Column,
    right_on: &Column,
    join_type: &JoinType,
) -> Vec<serde_json::Value> {
    let mut stuff = HashMap::new();

    // add all the relevent `on` values to map,
    for left_row in &left_rows {
        let left_object = left_row.as_object().unwrap();
        let value = left_object.get(&left_on.name).unwrap();

        stuff.insert(calculate_hash(value), vec![]);
    }

    // collect all the different right side values
    for right_row in right_rows {
        let right_object = right_row.as_object().unwrap();
        let value = right_object.get(&right_on.name).unwrap();

        // this assumes left join and ignores where there's no left match
        if let Some(items) = stuff.get_mut(&calculate_hash(value)) {
            items.push(right_object.clone());
        }
    }

    let mut output_rows = vec![];

    for left_row in left_rows {
        let left_object = left_row.as_object().unwrap();
        let hash = calculate_hash(left_object.get(&left_on.name).unwrap());

        if let Some(rhs) = stuff.get(&hash) {
            if rhs.is_empty() {
                // if left outer join
                if let JoinType::LeftOuter = join_type {
                    let whole_row = left_object.clone();
                    output_rows.push(serde_json::Value::Object(whole_row));
                }
            } else {
                for item in rhs {
                    let mut whole_row = left_object.clone();
                    whole_row.extend(item.clone());
                    output_rows.push(serde_json::Value::Object(whole_row));
                }
            }
        }
    }

    output_rows
}

fn apply_predicate(row: &Row, schema: &Schema, where_expr: &Expr) -> bool {
    match where_expr {
        Expr::ColumnComparison {
            column,
            op,
            literal,
        } => {
            let value = row.get_column(&column, schema).unwrap();

            match op {
                Op::Equals => value == literal,
            }
        }
    }
}

// filter columns out of a row
fn project_fields(row: Row, schema: &Schema, fields: &[Column]) -> Row {
    let mut items = vec![];

    for field in fields {
        let item = row.get_column(field, schema).unwrap();
        items.push(item.clone());
    }

    Row { items }
}

#[cfg(test)]
mod tests {
    use crate::{parser::parse, run_query};

    #[test]
    fn test_query_select_animals() {
        let query = parse("SELECT * FROM animal").unwrap();

        insta::assert_json_snapshot!(run_query(&query).to_json());
    }

    #[test]
    fn test_query_select_horse() {
        let query = parse("select * from animal where animal_name = 'horse'").unwrap();

        insta::assert_json_snapshot!(run_query(&query).to_json());
    }

    #[test]
    fn test_query_projection() {
        let query = parse("select animal_name from animal where animal_name = 'horse'").unwrap();

        insta::assert_json_snapshot!(run_query(&query).to_json());
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

        insta::assert_json_snapshot!(run_query(&query).to_json());
    }

    #[test]
    fn test_select_species_and_animals() {
        let query = parse(
            r#"
        select * from species 
          join animal on species_id
        where
          species_id = 3 
    "#,
        )
        .unwrap();

        insta::assert_json_snapshot!(run_query(&query).to_json());
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

        insta::assert_json_snapshot!(run_query(&query).to_json());
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

        insta::assert_json_snapshot!(run_query(&query).to_json());
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

        insta::assert_json_snapshot!(run_query(&query).to_json());
    }
}
