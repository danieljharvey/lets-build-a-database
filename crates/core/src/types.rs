use std::hash::Hash;

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash, Clone)]
pub struct Column {
    pub name: String,
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
