use std::hash::Hash;


#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub struct Column {
   pub  name: String,
}

#[derive(Debug)]
pub enum Expr {
    ColumnComparison {
        column: Column,
        op: Op,
        literal: serde_json::Value,
    },
}

#[derive(Debug)]
pub enum Op {
    Equals,
}

#[derive(Debug)]
pub struct Join {
    pub join_type: JoinType,
    pub left_from: Box<Query>,
    pub right_from: Box<Query>,
    pub left_column_on: Column,
    pub right_column_on: Column,
}

#[derive(Debug)]
pub struct TableName(pub String);

#[derive(Debug)]
pub struct From {
    pub table_name: TableName,
}

#[derive(Debug)]
pub struct Filter {
    pub from: Box<Query>,
    pub filter: Expr,
}

#[derive(Debug)]
pub enum JoinType {
    LeftInner,
    LeftOuter,
}

#[derive(Debug)]
pub enum Query {
    From(From),
    Filter(Filter),
    Join(Join),
}

