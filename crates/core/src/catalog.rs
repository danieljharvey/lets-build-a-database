use std::collections::BTreeMap;

use crate::{
    indexes::{construct_index, ConstructedIndexes, Index},
    query::raw_rows_for_table,
    types::{ColumnName, TableName},
};

pub struct Table {
    pub columns: Vec<ColumnName>,
    pub indexes: Vec<Index>,
}

pub struct Catalog {
    pub tables: BTreeMap<TableName, Table>,
}

impl Catalog {
    pub fn construct_indexes(&self) -> ConstructedIndexes {
        let mut constructed_indexes = BTreeMap::new();
        for (table_name, table) in &self.tables {
            let mut indexes = vec![];
            for index in &table.indexes {
                let rows = raw_rows_for_table(&index.table_name);
                indexes.push((index.clone(), construct_index(index, &rows)))
            }
            constructed_indexes.insert(table_name.clone(), indexes);
        }
        ConstructedIndexes {
            indexes: constructed_indexes,
        }
    }
}

// columns and indexes for each table
pub fn get_static_catalog() -> Catalog {
    let animal = Table {
        columns: vec![
            "animal_id".into(),
            "animal_name".into(),
            "species_id".into(),
        ],
        indexes: vec![
            Index {
                table_name: "animal".into(),
                columns: vec!["animal_id".into()],
            },
            Index {
                table_name: "animal".into(),
                columns: vec!["species_id".into()],
            },
        ],
    };

    let species = Table {
        columns: vec!["species_id".into(), "species_name".into()],
        indexes: vec![Index {
            table_name: "species".into(),
            columns: vec!["species_id".into()],
        }],
    };

    let album = Table {
        columns: vec!["AlbumId".into(), "Title".into(), "ArtistId".into()],
        indexes: vec![
            Index {
                table_name: "Album".into(),
                columns: vec!["AlbumId".into()],
            },
            Index {
                table_name: "Album".into(),
                columns: vec!["ArtistId".into()],
            },
        ],
    };

    let artist = Table {
        columns: vec!["ArtistId".into(), "Name".into()],
        indexes: vec![Index {
            table_name: "Artist".into(),
            columns: vec!["ArtistId".into()],
        }],
    };

    let track = Table {
        columns: vec![
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
        indexes: vec![
            Index {
                table_name: "Track".into(),
                columns: vec!["TrackId".into()],
            },
            Index {
                table_name: "Track".into(),
                columns: vec!["AlbumId".into()],
            },
            Index {
                table_name: "Track".into(),
                columns: vec!["MediaTypeId".into()],
            },
            Index {
                table_name: "Track".into(),
                columns: vec!["GenreId".into()],
            },
        ],
    };

    Catalog {
        tables: BTreeMap::from_iter([
            ("animal".into(), animal),
            ("species".into(), species),
            ("Album".into(), album),
            ("Artist".into(), artist),
            ("Track".into(), track),
        ]),
    }
}
