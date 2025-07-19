use serde_json::json;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::DefaultHasher;
use std::hash::Hasher;

fn main() {}

struct Table {
    name: String,
    columns: Vec<(String, Type)>,
    //indexes: Vec<Index>
}

struct Index {
    column: String,
}

enum Type {
    Int,
    String,
}

fn table_scan(table_name: &TableName) -> Vec<serde_json::Value> {
    match table_name.0.as_str() {
        "animal" => vec![(1, "horse", 1), (2, "dog", 1), (3, "snake", 2)]
            .iter()
            .map(|(id, name, species)| json!({ "animal_id": id, "animal_name": name, "species_id": species }))
            .collect(),
        "species" => vec![(1, "mammal"), (2, "reptile"), (3, "bird")]
            .iter()
            .map(|(id, name)| json!({"species_id": id, "species_name": name}))
            .collect(),
        _ => todo!("table not found {table_name:?}"),
    }
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
struct Column {
    name: String,
}

#[derive(Debug)]
enum Expr {
    ColumnComparison {
        column: Column,
        op: Op,
        literal: serde_json::Value,
    },
}

#[derive(Debug)]
enum Op {
    Equals,
}

#[derive(Debug)]
struct LeftJoin {
    left_from: Box<Query>,
    right_from: Box<Query>,
    left_column_on: Column,
    right_column_on: Column,
}

#[derive(Debug)]
struct TableName(pub String);

#[derive(Debug)]
struct From {
    table_name: TableName,
}

#[derive(Debug)]
struct Filter {
    from: Box<Query>,
    filter: Expr,
}

#[derive(Debug)]
enum Query {
    From(From),
    Filter(Filter),
    LeftJoin(LeftJoin),
}

fn run_query(query: &Query) -> Vec<serde_json::Value> {
    match query {
        Query::From(From { table_name }) => table_scan(table_name),
        Query::Filter(Filter { from, filter }) => run_query(&from)
            .into_iter()
            .filter(|row| apply_predicate(row, filter))
            .collect(),
        Query::LeftJoin(LeftJoin { left_from, right_from , left_column_on, right_column_on }) => {
            let left_rows = run_query(&left_from);
            let right_rows = run_query(&right_from); 

            hash_join(left_rows,right_rows,left_column_on,right_column_on)            
        }
    }
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

// implementation for left inner join
// todo: generalise later
fn hash_join(left_rows: Vec<serde_json::Value>, right_rows: Vec<serde_json::Value>, left_on: &Column, right_on: &Column) -> Vec<serde_json::Value> {
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
        match stuff.get_mut(&calculate_hash(value)) {
            Some(items) => {
                items.push(right_object.clone());
            },
            None => {}
        }
    }
   
    let mut output_rows = vec![];

    for left_row in left_rows {
        let left_object = left_row.as_object().unwrap();
        let hash = calculate_hash(left_object.get(&left_on.name).unwrap());
        
        if let Some(rhs) = stuff.get(&hash) {
                if !rhs.is_empty() {
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
    let query = Query::From(From {
        table_name: TableName("animal".to_string()),
    });
    assert_eq!(
        run_query(&query),
        vec![
            json!({"animal_id":1,"animal_name":"horse","species_id":1}),
            json!({"animal_id":2,"animal_name":"dog","species_id":1}),
            json!({"animal_id":3,"animal_name":"snake","species_id":2})
        ]
    );
}

#[test]
fn test_query_select_horse() {
    let query = Query::Filter(Filter {
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
    });
    assert_eq!(
        run_query(&query),
        vec![json!({"animal_id":1,"animal_name":"horse","species_id":1})]
    );
}

#[test]
fn test_select_horse_and_species() {
    let query = Query::LeftJoin(LeftJoin {
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
            table_name: TableName("species".to_string())
        })),
        left_column_on: Column {
            name: "species_id".to_string(),
        },
        right_column_on: Column {
            name: "species_id".to_string(),
        },
    });

    assert_eq!(
        run_query(&query,),
        vec![json!({"animal_id":1,"animal_name":"horse","species_id":1,"species_name":"mammal"})]
    );
}
