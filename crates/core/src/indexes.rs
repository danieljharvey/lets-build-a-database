use std::collections::BTreeMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use crate::types::{ColumnName, TableName};

#[derive(Debug, PartialEq)]
pub struct LineNumber(pub usize);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HashedValue(pub u64);

// the index we want
#[derive(Debug, PartialEq, Clone)]
pub struct Index {
    pub table_name: TableName,
    pub columns: Vec<ColumnName>,
}

// the items
#[derive(Debug, PartialEq)]
pub struct ConstructedIndex {
    items: BTreeMap<HashedValue, Vec<LineNumber>>,
}

pub fn construct_index(index: &Index, rows: &[serde_json::Value]) -> ConstructedIndex {
    let mut items: BTreeMap<HashedValue, Vec<LineNumber>> = BTreeMap::new();

    for (i, row) in rows.iter().enumerate() {
        let mut values = vec![];
        for column in &index.columns {
            if let Some(value) = row.get(column.0.as_str()) {
                values.push(value.clone());
            } else {
                values.push(serde_json::Value::Null);
            }
        }

        let hash = HashedValue(calculate_hash(&serde_json::Value::Array(values)));

        if let Some(line_numbers) = items.get_mut(&hash) {
            // add another item
            line_numbers.push(LineNumber(i));
        } else {
            items.insert(hash, vec![LineNumber(i)]);
        }
    }
    ConstructedIndex { items }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::{
        indexes::{
            calculate_hash, construct_index, ConstructedIndex, HashedValue, Index, LineNumber,
        },
        types::{ColumnName, TableName},
    };

    #[test]
    fn test_construct_index() {
        let rows = vec![
            json!({"id": 1}),
            json!({"id": 2}),
            json!({"id": 3}),
            json!({"id": 3}),
        ];

        let index = Index {
            table_name: TableName("test".into()),
            columns: vec![ColumnName("id".into())],
        };

        let one_hash = HashedValue(calculate_hash(&json!([1])));
        let two_hash = HashedValue(calculate_hash(&json!([2])));
        let three_hash = HashedValue(calculate_hash(&json!([3])));

        let expected = ConstructedIndex {
            items: BTreeMap::from_iter([
                (one_hash, vec![LineNumber(0)]),
                (two_hash, vec![LineNumber(1)]),
                (three_hash, vec![LineNumber(2), LineNumber(3)]),
            ]),
        };

        assert_eq!(construct_index(&index, &rows), expected)
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}
