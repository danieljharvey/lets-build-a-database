use std::collections::BTreeMap;

use crate::{
    indexes::Index,
    types::{ColumnName, TableName},
};

pub struct Table {
    columns: Vec<ColumnName>,
    indexes: Vec<Index>,
}

pub struct Catalog {
    tables: BTreeMap<TableName, Table>,
}

// columns and indexes for each table
pub fn get_static_catalog() -> Catalog {
    let table_names = vec!["animal", "species", "Album", "Artist", "Track"];

    let animal = Table {
        columns: vec![],
        indexes: vec![],
    };

    Catalog {
        tables: BTreeMap::from_iter([("animal".into(), animal)]),
    }
}
