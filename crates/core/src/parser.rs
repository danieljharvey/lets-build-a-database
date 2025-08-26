use sqlparser::ast::{self, LimitClause, OrderByKind};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

use crate::types::{
    Column, Expr, Filter, From, Join, JoinOn, JoinType, Limit, Op, Order, OrderBy, OrderByExpr,
    Project, Query, TableAlias, TableName,
};

#[derive(Debug)]
pub enum ParseError {
    NoStatements,
    OnlyQueryIsSupported,
    WithNotSupported,
    OffsetNotSupported,
    OffsetCommaLimitNotSupported,
    LimitByNotSupported,
    LimitMustBeInt,
    FetchNotSupported,
    LocksNotSupported,
    ForClauseNotSupported,
    SettingsNotSupported,
    FormatNotSupported,
    PipeOperatorsNotSupported,
    OnlySelectIsSupported,
    IntoNotSupported,
    EmptyFromNotSupported,
    DistinctNotSupported,
    TableOnlyInFrom,
    EmptyObjectName,
    UnknownExprPart { expr: String },
    GroupByNotSupported,
    SortByNotSupported,
    ExpectedIdent,
    ExpectedTwoIdents,
    UnsupportedProjectionField,
    TableAliasColumnsNotSupported,
    Join(JoinParseError),
    OrderBy(OrderByParseError),
    ExpectedValue(ast::Expr),
    SerdeJsonError(String, serde_json::Error),
    UnknownOperator,
}

#[derive(Debug)]
pub enum OrderByParseError {
    UnsupportedOrderBy,
    NullsFirstNotSupported,
}

#[derive(Debug)]
pub enum JoinParseError {
    UnsupportedJoinOperator,
    UnsupportedJoinConstraint,
}

impl std::convert::From<JoinParseError> for ParseError {
    fn from(join_parse_error: JoinParseError) -> ParseError {
        ParseError::Join(join_parse_error)
    }
}

pub fn parse(sql: &str) -> Result<Query, ParseError> {
    let dialect = AnsiDialect {}; // or AnsiDialect

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    if let Some(first_statement) = ast.first() {
        from_statement(first_statement)
    } else {
        Err(ParseError::NoStatements)
    }
}

fn from_statement(statement: &ast::Statement) -> Result<Query, ParseError> {
    match statement {
        ast::Statement::Query(query) => from_query(query),
        _ => Err(ParseError::OnlyQueryIsSupported),
    }
}

fn from_query(query: &ast::Query) -> Result<Query, ParseError> {
    let ast::Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    if with.is_some() {
        return Err(ParseError::WithNotSupported);
    }

    if fetch.is_some() {
        return Err(ParseError::FetchNotSupported);
    }

    if !locks.is_empty() {
        return Err(ParseError::LocksNotSupported);
    }

    if for_clause.is_some() {
        return Err(ParseError::ForClauseNotSupported);
    }

    if settings.is_some() {
        return Err(ParseError::SettingsNotSupported);
    }

    if format_clause.is_some() {
        return Err(ParseError::FormatNotSupported);
    }

    if !pipe_operators.is_empty() {
        return Err(ParseError::PipeOperatorsNotSupported);
    }

    let mut query = from_body(body)?;

    if let Some(order_by) = order_by {
        query = Query::OrderBy(OrderBy {
            from: Box::new(query),
            order_by_exprs: from_order_by(order_by)?,
        })
    }

    if let Some(limit) = limit_clause {
        query = Query::Limit(Limit {
            from: Box::new(query),
            limit: from_limit(limit)?,
        });
    }

    Ok(query)
}

fn from_order_by(order_by: &ast::OrderBy) -> Result<Vec<OrderByExpr>, ParseError> {
    let OrderByKind::Expressions(order_by_expressions) = &order_by.kind else {
        return Err(ParseError::OrderBy(OrderByParseError::UnsupportedOrderBy));
    };

    order_by_expressions
        .iter()
        .map(|expr| {
            let column = identifier_from_selection(&expr.expr)?;
            let order = if expr.options.asc.unwrap_or(true) {
                Order::Asc
            } else {
                Order::Desc
            };
            if expr.options.nulls_first.is_some() {
                return Err(ParseError::OrderBy(
                    OrderByParseError::NullsFirstNotSupported,
                ));
            }
            Ok(OrderByExpr { column, order })
        })
        .collect()
}

fn from_limit(limit: &ast::LimitClause) -> Result<u64, ParseError> {
    match limit {
        LimitClause::OffsetCommaLimit { .. } => Err(ParseError::OffsetCommaLimitNotSupported),
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if offset.is_some() {
                return Err(ParseError::OffsetNotSupported);
            }

            if !limit_by.is_empty() {
                return Err(ParseError::LimitByNotSupported);
            }

            if let Some(limit) = limit {
                let value = value_from_selection(limit)?;

                match value {
                    serde_json::Value::Number(a) => a.as_u64().ok_or(ParseError::LimitMustBeInt),
                    _ => Err(ParseError::LimitMustBeInt),
                }
            } else {
                Err(ParseError::LimitMustBeInt)
            }
        }
    }
}

fn from_body(body: &ast::SetExpr) -> Result<Query, ParseError> {
    match body {
        ast::SetExpr::Select(select) => from_select(select),
        _ => Err(ParseError::OnlySelectIsSupported),
    }
}

fn from_select(select: &ast::Select) -> Result<Query, ParseError> {
    let ast::Select {
        select_token: _,
        distinct,
        top: _,
        top_before_distinct: _,
        projection,
        into,
        from,
        lateral_views: _,
        prewhere: _,
        selection,
        group_by,
        cluster_by: _,
        distribute_by: _,
        sort_by,
        having: _,
        named_window: _,
        qualify: _,
        window_before_qualify: _,
        value_table_mode: _,
        connect_by: _,
        flavor: _,
    } = select;

    if into.is_some() {
        return Err(ParseError::IntoNotSupported);
    }

    if distinct.is_some() {
        return Err(ParseError::DistinctNotSupported);
    }

    match group_by {
        ast::GroupByExpr::All(a) => {
            if !a.is_empty() {
                return Err(ParseError::GroupByNotSupported);
            }
        }
        ast::GroupByExpr::Expressions(a, b) => {
            if !a.is_empty() || !b.is_empty() {
                return Err(ParseError::GroupByNotSupported);
            }
        }
    }

    if !sort_by.is_empty() {
        return Err(ParseError::SortByNotSupported);
    }

    let mut query = from_from(from)?;

    if let Some(filter) = selection.as_ref().map(from_selection).transpose()? {
        query = Query::Filter(Filter {
            filter,
            from: Box::new(query),
        });
    }

    if let Some(fields) = from_projection(projection)? {
        query = Query::Project(Project {
            from: Box::new(query),
            fields,
        });
    }

    Ok(query)
}

fn identifier_from_selection(expr: &ast::Expr) -> Result<Column, ParseError> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(Column {
            name: ident.value.to_string(),
            table_alias: None,
        }),
        ast::Expr::CompoundIdentifier(idents) => {
            if let (Some(table_alias), Some(column), None) =
                (idents.first(), idents.get(1), idents.get(2))
            {
                Ok(Column {
                    name: column.to_string(),
                    table_alias: Some(TableAlias(table_alias.to_string())),
                })
            } else {
                Err(ParseError::ExpectedTwoIdents)
            }
        }
        _ => Err(ParseError::ExpectedIdent),
    }
}

fn from_binary_operator(op: &ast::BinaryOperator) -> Result<Op, ParseError> {
    match op {
        ast::BinaryOperator::Eq => Ok(Op::Equals),
        ast::BinaryOperator::Gt => Ok(Op::GreaterThan),
        ast::BinaryOperator::GtEq => Ok(Op::GreaterThanOrEqual),
        ast::BinaryOperator::Lt => Ok(Op::LessThan),
        ast::BinaryOperator::LtEq => Ok(Op::LessThanOrEqual),
        ast::BinaryOperator::Plus => Ok(Op::Add),
        ast::BinaryOperator::Minus => Ok(Op::Subtract),
        _ => Err(ParseError::UnknownOperator),
    }
}

fn value_from_selection(expr: &ast::Expr) -> Result<serde_json::Value, ParseError> {
    match expr {
        ast::Expr::Value(value) => {
            let ast::ValueWithSpan {
                value: inner_value, ..
            } = value;
            if let ast::Value::SingleQuotedString(s) = inner_value {
                Ok(s.clone().into())
            } else {
                // last resort, stringify the thing and throw it at serde_json decode
                let val_string = value.to_string();
                serde_json::from_str(val_string.as_str())
                    .map_err(|e| ParseError::SerdeJsonError(val_string, e))
            }
        }
        _ => Err(ParseError::ExpectedValue(expr.clone())),
    }
}

fn from_selection(expr: &ast::Expr) -> Result<Expr, ParseError> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOperation {
            left: Box::new(from_selection(left)?),
            op: from_binary_operator(op)?,
            right: Box::new(from_selection(right)?),
        }),
        ast::Expr::Value(value) => {
            let ast::ValueWithSpan {
                value: inner_value, ..
            } = value;
            let literal = if let ast::Value::SingleQuotedString(s) = inner_value {
                Ok(s.clone().into())
            } else {
                // last resort, stringify the thing and throw it at serde_json decode
                let val_string = value.to_string();
                serde_json::from_str(val_string.as_str())
                    .map_err(|e| ParseError::SerdeJsonError(val_string, e))
            }?;
            Ok(Expr::Literal { literal })
        }
        ast::Expr::Identifier(ident) => Ok(Expr::Column {
            column: Column {
                name: ident.value.to_string(),
                table_alias: None,
            },
        }),
        ast::Expr::CompoundIdentifier(idents) => {
            if let (Some(table_alias), Some(column), None) =
                (idents.first(), idents.get(1), idents.get(2))
            {
                Ok(Expr::Column {
                    column: Column {
                        name: column.to_string(),
                        table_alias: Some(TableAlias(table_alias.to_string())),
                    },
                })
            } else {
                Err(ParseError::ExpectedTwoIdents)
            }
        }
        ast::Expr::Nested(expr) => Ok(Expr::Nested {
            expr: Box::new(from_selection(expr)?),
        }),
        _ => {
            dbg!(&expr);
            Err(ParseError::UnknownExprPart {
                expr: expr.to_string(),
            })
        }
    }
}

fn from_from(froms: &[ast::TableWithJoins]) -> Result<Query, ParseError> {
    if let Some(table_with_joins) = froms.iter().next() {
        let ast::TableWithJoins { relation, joins } = table_with_joins;
        let from = from_relation(relation)?;

        joins
            .iter()
            .try_fold(Query::From(from), |query, join| from_join(join, query))
    } else {
        Err(ParseError::EmptyFromNotSupported)
    }
}

fn from_table_alias(table_alias: &ast::TableAlias) -> Result<TableAlias, ParseError> {
    if table_alias.columns.is_empty() {
        Ok(TableAlias(table_alias.name.value.clone()))
    } else {
        Err(ParseError::TableAliasColumnsNotSupported)
    }
}

fn from_relation(table: &ast::TableFactor) -> Result<From, ParseError> {
    if let ast::TableFactor::Table {
        name,
        alias,
        args: _,
        with_hints: _,
        version: _,
        with_ordinality: _,
        partitions: _,
        json_path: _,
        sample: _,
        index_hints: _,
    } = table
    {
        let table_name = table_name_from_object_name(name)?;

        let table_alias = alias.as_ref().map(from_table_alias).transpose()?;

        Ok(From {
            table_name,
            table_alias,
        })
    } else {
        Err(ParseError::TableOnlyInFrom)
    }
}

fn from_join(join: &ast::Join, query: Query) -> Result<Query, ParseError> {
    let from = from_relation(&join.relation)?;

    let (join_type, left_column_on, right_column_on) = from_join_operator(&join.join_operator)?;

    let right_column_on = Column {
        table_alias: from.table_alias.clone(),
        ..right_column_on
    };

    let join = Join {
        join_type,
        left_from: Box::new(query),
        right_from: Box::new(Query::From(from)),
        on: JoinOn {
            left: left_column_on,
            right: right_column_on,
        },
    };

    Ok(Query::Join(join))
}

fn from_join_operator(
    join_operator: &ast::JoinOperator,
) -> Result<(JoinType, Column, Column), ParseError> {
    let (join_type, constraint) = match join_operator {
        ast::JoinOperator::Join(constraint) => Ok((JoinType::Inner, constraint)),
        ast::JoinOperator::LeftOuter(constraint) => Ok((JoinType::LeftOuter, constraint)),
        _ => Err(JoinParseError::UnsupportedJoinOperator),
    }?;

    match constraint {
        ast::JoinConstraint::On(expr) => {
            let identifier = identifier_from_selection(expr)?;
            Ok((join_type, identifier.clone(), identifier))
        }
        _ => Err(ParseError::from(JoinParseError::UnsupportedJoinConstraint)),
    }
}

fn table_name_from_object_name(object_name: &ast::ObjectName) -> Result<TableName, ParseError> {
    let ast::ObjectName(object_name_parts) = object_name;

    if let Some(object_name_part) = object_name_parts.iter().next() {
        let ast::ObjectNamePart::Identifier(name) = object_name_part;
        Ok(TableName(name.value.to_string()))
    } else {
        Err(ParseError::EmptyObjectName)
    }
}

fn from_projection(select_items: &[ast::SelectItem]) -> Result<Option<Vec<Column>>, ParseError> {
    if select_items.len() == 1 {
        if let Some(ast::SelectItem::Wildcard(_)) = select_items.first() {
            return Ok(None);
        }
    }

    let mut fields = vec![];

    for select_item in select_items {
        match select_item {
            ast::SelectItem::UnnamedExpr(expr) => {
                let identifier = identifier_from_selection(expr)?;
                fields.push(identifier);
            }
            _ => return Err(ParseError::UnsupportedProjectionField),
        }
    }
    Ok(Some(fields))
}

#[cfg(test)]
mod tests {
    use crate::types::{Column, Expr, Filter, From, Join, JoinOn, JoinType, Op, Query, TableName};

    use super::parse;

    #[test]
    fn test_parse_basic_select() {
        let expected = Query::From(From {
            table_name: TableName("albums".into()),
            table_alias: None,
        });

        let result = parse("SELECT * FROM albums").unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_basic_select_with_where() {
        let expected = Query::Filter(Filter {
            from: Box::new(Query::From(From {
                table_name: TableName("albums".into()),
                table_alias: None,
            })),
            filter: Expr::BinaryOperation {
                left: Box::new(Expr::Column {
                    column: Column {
                        name: "album_id".to_string(),
                        table_alias: None,
                    },
                }),
                op: Op::Equals,
                right: Box::new(Expr::Literal { literal: 1.into() }),
            },
        });

        let result = parse("SELECT * FROM albums WHERE album_id = 1").unwrap();

        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_basic_join() {
        let expected = Query::Filter(Filter {
            from: Box::new(Query::Join(Join {
                join_type: JoinType::Inner,
                left_from: Box::new(Query::From(From {
                    table_name: TableName("species".to_string()),
                    table_alias: None,
                })),
                right_from: Box::new(Query::From(From {
                    table_name: TableName("animal".to_string()),
                    table_alias: None,
                })),
                on: JoinOn {
                    left: Column {
                        name: "species_id".to_string(),
                        table_alias: None,
                    },
                    right: Column {
                        name: "species_id".to_string(),
                        table_alias: None,
                    },
                },
            })),
            filter: Expr::BinaryOperation {
                left: Box::new(Expr::Column {
                    column: Column {
                        name: "species_id".to_string(),
                        table_alias: None,
                    },
                }),
                op: Op::Equals,
                right: Box::new(Expr::Literal { literal: 3.into() }),
            },
        });

        let result = parse(
            r"
            SELECT * FROM species
            JOIN 
              animal ON species_id
            WHERE
              species_id = 3
            ",
        )
        .unwrap();

        assert_eq!(result, expected);
    }
}
