use sqlparser::ast::{self};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;

use crate::types::{Column, Expr, Filter, From, Op, Query, TableName};

#[derive(Debug)]
pub enum ParseError {
    NoStatements,
    JoinsNotSupported,
    OnlyQueryIsSupported,
    WithNotSupported,
    OrderByNotSupported,
    LimitClauseNotSupported,
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
    UnknownExprPart,
    GroupByNotSupported,
    SortByNotSupported,
    ExpectedIdent,
    ExpectedValue(ast::Expr),
    SerdeJsonError(String, serde_json::Error),
    UnknownBinaryOperator,
}

pub fn parse(sql: &str) -> Result<Query, ParseError> {
    let dialect = AnsiDialect {}; // or AnsiDialect

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    if let Some(first_statement) = ast.iter().next() {
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

    if order_by.is_some() {
        return Err(ParseError::OrderByNotSupported);
    }

    if limit_clause.is_some() {
        return Err(ParseError::LimitClauseNotSupported);
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

    from_body(body)
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

    from_projection(projection)?;

    let mut query = Query::From(from_from(from)?);

    if let Some(filter) = selection
        .as_ref()
        .map(|expr| from_selection(expr))
        .transpose()?
    {
        query = Query::Filter(Filter {
            filter,
            from: Box::new(query),
        });
    }

    Ok(query)
}

fn identifier_from_selection(expr: &ast::Expr) -> Result<Column, ParseError> {
    match expr {
        ast::Expr::Identifier(ident) => Ok(Column {
            name: ident.value.to_string(),
        }),
        _ => Err(ParseError::ExpectedIdent),
    }
}

fn from_binary_operator(op: &ast::BinaryOperator) -> Result<Op, ParseError> {
    match op {
        ast::BinaryOperator::Eq => Ok(Op::Equals),
        _ => Err(ParseError::UnknownBinaryOperator),
    }
}

fn value_from_selection(expr: &ast::Expr) -> Result<serde_json::Value, ParseError> {
    match expr {
        ast::Expr::Value(value) => {
            let ast::ValueWithSpan {
                value: inner_value, ..
            } = value;
            match inner_value {
                ast::Value::SingleQuotedString(s) => Ok(s.clone().into()),
                _ => {
                    // last resort, stringify the thing and throw it at serde_json decode
                    let val_string = value.to_string();
                    serde_json::from_str(val_string.as_str())
                        .map_err(|e| ParseError::SerdeJsonError(val_string, e))
                }
            }
        }
        _ => Err(ParseError::ExpectedValue(expr.clone())),
    }
}

fn from_selection(expr: &ast::Expr) -> Result<Expr, ParseError> {
    match expr {
        ast::Expr::BinaryOp { left, op, right } => Ok(Expr::ColumnComparison {
            column: identifier_from_selection(left)?,
            op: from_binary_operator(op)?,
            literal: value_from_selection(right)?,
        }),
        _ => Err(ParseError::UnknownExprPart),
    }
}

fn from_from(froms: &Vec<ast::TableWithJoins>) -> Result<From, ParseError> {
    if let Some(table_with_joins) = froms.iter().next() {
        let ast::TableWithJoins { relation, joins } = table_with_joins;
        if !joins.is_empty() {
            return Err(ParseError::JoinsNotSupported);
        }
        if let ast::TableFactor::Table {
            name,
            alias: _,
            args: _,
            with_hints: _,
            version: _,
            with_ordinality: _,
            partitions: _,
            json_path: _,
            sample: _,
            index_hints: _,
        } = relation
        {
            let table_name = table_name_from_object_name(name)?;
            Ok(From { table_name })
        } else {
            Err(ParseError::TableOnlyInFrom)
        }
    } else {
        Err(ParseError::EmptyFromNotSupported)
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

fn from_projection(_projection: &Vec<ast::SelectItem>) -> Result<(), ParseError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::types::{Column, Expr, Filter, From, Op, Query, TableName};

    use super::parse;

    #[test]
    fn test_parse_basic_select() {
        let expected = Query::From(From {
            table_name: TableName("albums".into()),
        });

        let result = parse("SELECT * FROM albums").unwrap();

        assert_eq!(result, expected)
    }

    #[test]
    fn test_parse_basic_select_with_where() {
        let expected = Query::Filter(Filter {
            from: Box::new(Query::From(From {
                table_name: TableName("albums".into()),
            })),
            filter: Expr::ColumnComparison {
                column: Column {
                    name: "album_id".to_string(),
                },
                op: Op::Equals,
                literal: 1.into(),
            },
        });

        let result = parse("SELECT * FROM albums WHERE album_id = 1").unwrap();

        assert_eq!(result, expected)
    }
}
