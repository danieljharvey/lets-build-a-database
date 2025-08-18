use crate::catalog::Catalog;
use crate::indexes::Index;
use crate::types::Cost;
use crate::types::QueryStep;
use crate::types::Row;
use crate::types::Schema;
use crate::types::TableAlias;
use crate::types::{Column, ColumnName, TableName};
use serde_json::json;

// hard coded vec of column names for now
fn schema(table_name: &TableName, catalog: &Catalog) -> Vec<ColumnName> {
    catalog.tables.get(table_name).unwrap().columns.clone()
}

fn split_to_rows(str: &str) -> Vec<serde_json::Value> {
    str.lines()
        .map(|row| serde_json::from_str::<serde_json::Value>(row).unwrap())
        .collect()
}

pub fn raw_rows_for_table(table_name: &TableName) -> Vec<serde_json::Value> {
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
            split_to_rows(include_str!("../../static/Album.jsonl"))
        },
        "Artist" => {
            split_to_rows(include_str!("../../static/Artist.jsonl"))
        }
        "Track" => {
            split_to_rows(include_str!("../../static/Track.jsonl"))
        }
        _ => todo!("table not found {table_name:?}"),
    }
}

// scan of static values for now
pub fn table_scan(
    table_name: &TableName,
    table_alias: Option<&TableAlias>,
    catalog: &Catalog,
) -> QueryStep {
    let columns = schema(table_name, catalog)
        .into_iter()
        .map(|name| Column {
            table_alias: table_alias.cloned(),
            name,
        })
        .collect();

    let raw = raw_rows_for_table(table_name);

    let mut cost = Cost::new();

    let rows = raw
        .into_iter()
        .map(|raw| {
            cost.increment_rows_processed();
            into_row(raw, &columns)
        })
        .collect();

    QueryStep {
        schema: Schema { columns },
        rows,
        cost,
    }
}

// scan of static values for now
pub fn index_scan(
    table_name: &TableName,
    table_alias: Option<&TableAlias>,
    index: &Index,
    values: &Vec<serde_json::Value>,
    catalog: &Catalog,
) -> QueryStep {
    let columns = schema(table_name, catalog)
        .into_iter()
        .map(|name| Column {
            table_alias: table_alias.cloned(),
            name,
        })
        .collect();

    

    let raw = raw_rows_for_table(table_name).into_iter().;
    
    // get only relevant rows from `raw`



    let mut cost = Cost::new();

    let rows = raw
        .into_iter()
        .map(|raw| {
            cost.increment_rows_processed();
            into_row(raw, &columns)
        })
        .collect();

    QueryStep {
        schema: Schema { columns },
        rows,
        cost,
    }
}

fn into_row(value: serde_json::Value, columns: &Vec<Column>) -> Row {
    let serde_json::Value::Object(mut map) = value else {
        panic!("what is this")
    };

    let mut items = vec![];

    // collect items in order
    for column in columns {
        let Some(item) = map.remove(&column.name.0) else {
            panic!("could not find {}", column.name);
        };

        items.push(item);
    }

    Row { items }
}
