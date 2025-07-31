use std::{fmt::Display, hash::Hash};

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Clone)]
pub struct Column {
    pub name: String,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.name)
    }
}

impl std::convert::From<&str> for Column {
    fn from(name: &str) -> Column {
        Column {
            name: name.to_string(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    ColumnComparison {
        column: Column,
        op: Op,
        literal: serde_json::Value,
    },
}

#[derive(Debug, PartialEq)]
pub enum Op {
    Equals,
}

#[derive(Debug, PartialEq)]
pub struct Join {
    #[allow(clippy::struct_field_names)]
    pub join_type: JoinType,
    pub left_from: Box<Query>,
    pub right_from: Box<Query>,
    pub left_column_on: Column,
    pub right_column_on: Column,
}

#[derive(Debug, PartialEq)]
pub struct TableName(pub String);

#[derive(Debug, PartialEq)]
pub struct From {
    pub table_name: TableName,
}

#[derive(Debug, PartialEq)]
pub struct Filter {
    pub from: Box<Query>,
    pub filter: Expr,
}

#[derive(Debug, PartialEq)]
pub struct Project {
    pub from: Box<Query>,
    pub fields: Vec<Column>,
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
}

#[derive(Debug, PartialEq)]
pub enum Query {
    From(From),
    Filter(Filter),
    Join(Join),
    Project(Project),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Row {
    pub items: Vec<serde_json::Value>,
}

impl Row {
    pub fn get_column(&self, column: &Column, schema: &Schema) -> Option<&serde_json::Value> {
        let index = schema.get_index_for_column(column)?;

        self.items.get(index)
    }

    pub fn extend(&mut self, row: Row) {
        self.items.extend(row.items);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn get_index_for_column(&self, column: &Column) -> Option<usize> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column_name)| column_name == &column)
            .map(|(i, _)| i)
    }

    pub fn extend(&mut self, schema: Schema) {
        self.columns.extend(schema.columns);
    }
}

pub struct QueryStep {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl QueryStep {
    // reconstruct JSON output
    pub fn to_json(&self) -> serde_json::Value {
        let mut output_rows = vec![];

        for row in &self.rows {
            let mut output_row = serde_json::Map::new();
            for column in &self.schema.columns {
                let value = row.get_column(column, &self.schema).unwrap();
                output_row.insert(column.to_string(), value.clone());
            }
            output_rows.push(serde_json::Value::Object(output_row));
        }
        serde_json::Value::Array(output_rows)
    }
}
