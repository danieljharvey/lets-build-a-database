mod parser;
mod types;

use parser::parse;
use serde_json::json;
use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use types::{Column, Expr, Filter, From, Join, JoinType, Op, Query, TableName};

// scan of static values for now
fn table_scan(table_name: &TableName) -> Vec<serde_json::Value> {
    match table_name.0.as_str() {
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
            serde_json::from_str::<serde_json::Value>(my_str).unwrap().as_array().unwrap().clone()
        },
        "Artist" => {
            let my_str = include_str!("../static/Artist.json");
            serde_json::from_str::<serde_json::Value>(my_str).unwrap().as_array().unwrap().clone()
        }

        _ => todo!("table not found {table_name:?}"),
    }
}

pub fn run_query(query: &Query) -> Vec<serde_json::Value> {
    match query {
        Query::From(From { table_name }) => table_scan(table_name),
        Query::Filter(Filter { from, filter }) => run_query(from)
            .into_iter()
            .filter(|row| apply_predicate(row, filter))
            .collect(),
        Query::Join(Join {
            left_from,
            right_from,
            left_column_on,
            right_column_on,
            join_type,
        }) => {
            let left_rows = run_query(left_from);
            let right_rows = run_query(right_from);

            hash_join(
                left_rows,
                right_rows,
                left_column_on,
                right_column_on,
                join_type,
            )
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

fn apply_predicate(row: &serde_json::Value, where_expr: &Expr) -> bool {
    match where_expr {
        Expr::ColumnComparison {
            column,
            op,
            literal,
        } => {
            let row_object = row.as_object().unwrap();

            let value = row_object.get(&column.name).unwrap();

            match op {
                Op::Equals => value == literal,
            }
        }
    }
}

#[test]
fn test_query_select_animals() {
    let query = parse("SELECT * FROM animal").unwrap();

    insta::assert_json_snapshot!(run_query(&query));
}

#[test]
fn test_query_select_horse() {
    let query = parse("select * from animal where animal_name = 'horse'").unwrap();

    insta::assert_json_snapshot!(run_query(&query));
}

#[test]
fn test_select_horse_and_species() {
    let query = Query::Join(Join {
        join_type: JoinType::LeftInner,
        left_from: Box::new(Query::Filter(Filter {
            from: Box::new(Query::From(From {
                table_name: TableName("animal".to_string()),
            })),
            filter: Expr::ColumnComparison {
                column: Column {
                    name: "animal_name".to_string(),
                },
                op: Op::Equals,
                literal: "horse".into(),
            },
        })),
        right_from: Box::new(Query::From(From {
            table_name: TableName("species".to_string()),
        })),
        left_column_on: Column {
            name: "species_id".to_string(),
        },
        right_column_on: Column {
            name: "species_id".to_string(),
        },
    });

    insta::assert_json_snapshot!(run_query(&query));
}

#[test]
fn test_select_species_and_animals() {
    let query = Query::Join(Join {
        join_type: JoinType::LeftInner,
        left_from: Box::new(Query::Filter(Filter {
            from: Box::new(Query::From(From {
                table_name: TableName("species".to_string()),
            })),
            filter: Expr::ColumnComparison {
                column: Column {
                    name: "species_id".to_string(),
                },
                op: Op::Equals,
                literal: 3.into(),
            },
        })),
        right_from: Box::new(Query::From(From {
            table_name: TableName("animal".to_string()),
        })),
        left_column_on: Column {
            name: "species_id".to_string(),
        },
        right_column_on: Column {
            name: "species_id".to_string(),
        },
    });

    insta::assert_json_snapshot!(run_query(&query));
}

#[test]
fn test_select_species_and_animals_left_outer() {
    let query = Query::Join(Join {
        join_type: JoinType::LeftOuter,
        left_from: Box::new(Query::Filter(Filter {
            from: Box::new(Query::From(From {
                table_name: TableName("species".to_string()),
            })),
            filter: Expr::ColumnComparison {
                column: Column {
                    name: "species_id".to_string(),
                },
                op: Op::Equals,
                literal: 3.into(),
            },
        })),
        right_from: Box::new(Query::From(From {
            table_name: TableName("animal".to_string()),
        })),
        left_column_on: Column {
            name: "species_id".to_string(),
        },
        right_column_on: Column {
            name: "species_id".to_string(),
        },
    });

    insta::assert_json_snapshot!(run_query(&query));
}

#[test]
fn test_select_album() {
    let query = parse("select * from Album where Title = 'Jagged Little Pill'").unwrap();

    insta::assert_json_snapshot!(run_query(&query));
}

#[test]
fn test_select_album_and_artist() {
    let query = Query::Join(Join {
        join_type: JoinType::LeftInner,
        left_from: Box::new(Query::Filter(Filter {
            from: Box::new(Query::From(From {
                table_name: TableName("Album".to_string()),
            })),
            filter: Expr::ColumnComparison {
                column: Column {
                    name: "ArtistId".to_string(),
                },
                op: Op::Equals,
                literal: 82.into(),
            },
        })),
        right_from: Box::new(Query::From(From {
            table_name: TableName("Artist".to_string()),
        })),
        left_column_on: Column {
            name: "ArtistId".to_string(),
        },
        right_column_on: Column {
            name: "ArtistId".to_string(),
        },
    });

    insta::assert_json_snapshot!(run_query(&query));
}
