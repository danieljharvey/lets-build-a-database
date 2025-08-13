use std::{fmt::Display, hash::Hash};

use crate::indexes::Index;

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Clone)]
pub struct ColumnName(pub String);

impl std::convert::From<&str> for ColumnName {
    fn from(name: &str) -> Self {
        ColumnName(name.to_string())
    }
}

impl Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Clone)]
pub struct Column {
    pub name: ColumnName,
    pub table_alias: Option<TableAlias>,
}

impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match &self.table_alias {
            Some(table_alias) => {
                write!(f, "{}.{}", table_alias, self.name)
            }
            None => {
                write!(f, "{}", self.name)
            }
        }
    }
}

impl std::convert::From<&str> for Column {
    fn from(name: &str) -> Column {
        Column {
            name: ColumnName(name.to_string()),
            table_alias: None,
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
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, PartialEq)]
pub struct Join<Plan> {
    #[allow(clippy::struct_field_names)]
    pub join_type: JoinType,
    pub left_from: Box<Plan>,
    pub right_from: Box<Plan>,
    pub on: JoinOn,
}

#[derive(Debug, PartialEq)]
pub struct JoinOn {
    pub left: Column,
    pub right: Column,
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct TableName(pub String);

impl std::convert::From<&str> for TableName {
    fn from(name: &str) -> TableName {
        TableName(name.to_string())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct TableAlias(pub String);

impl Display for TableAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, PartialEq)]
pub struct From {
    pub table_name: TableName,
    pub table_alias: Option<TableAlias>,
}

#[derive(Debug, PartialEq)]
pub struct Filter<Plan> {
    pub from: Box<Plan>,
    pub filter: Expr,
}

#[derive(Debug, PartialEq)]
pub struct Project<Plan> {
    pub from: Box<Plan>,
    pub fields: Vec<Column>,
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
}

#[derive(Debug, PartialEq)]
pub struct Limit<Plan> {
    pub from: Box<Plan>,
    pub limit: u64,
}

#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    From(From),
    Filter(Filter<LogicalPlan>),
    Join(Join<LogicalPlan>),
    Project(Project<LogicalPlan>),
    Limit(Limit<LogicalPlan>),
}

#[derive(Debug, PartialEq)]
pub struct TableScan {
    pub table_name: TableName,
    pub table_alias: Option<TableAlias>,
}

#[derive(Debug, PartialEq)]
pub struct IndexScan {
    pub table_name: TableName,
    pub table_alias: Option<TableAlias>,
    pub index: Index,
    pub values: Vec<serde_json::Value>,
}

#[derive(Debug, PartialEq)]
pub enum PhysicalPlan {
    TableScan(TableScan),
    IndexScan(IndexScan),
    Filter(Filter<PhysicalPlan>),
    Join(Join<PhysicalPlan>),
    Project(Project<PhysicalPlan>),
    Limit(Limit<PhysicalPlan>),
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
    pub cost: Cost,
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

#[derive(Debug)]
pub struct Cost {
    pub rows_processed: u64,
}

impl Default for Cost {
    fn default() -> Self {
        Self::new()
    }
}

impl Cost {
    pub fn new() -> Self {
        Cost { rows_processed: 0 }
    }

    pub fn increment_rows_processed(&mut self) {
        self.rows_processed += 1;
    }

    pub fn extend(&mut self, cost: &Cost) {
        self.rows_processed += cost.rows_processed;
    }
}
