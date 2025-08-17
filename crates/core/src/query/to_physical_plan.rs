use std::collections::BTreeMap;

use crate::indexes::{ConstructedIndexes, Index};
use crate::types::{ColumnName, Expr, IndexScan, Limit, Op, PhysicalPlan, TableName, TableScan};

use crate::types::{Filter, From, Join, LogicalPlan, Project};

pub fn to_physical_plan(logical_plan: LogicalPlan, indexes: &ConstructedIndexes) -> PhysicalPlan {
    match logical_plan {
        LogicalPlan::From(From {
            table_name,
            table_alias,
        }) => PhysicalPlan::TableScan(TableScan {
            table_name,
            table_alias,
        }),
        LogicalPlan::Filter(filter) => filter_to_physical_plan(filter, indexes),
        LogicalPlan::Join(Join {
            join_type,
            left_from,
            right_from,
            on,
        }) => PhysicalPlan::Join(Join {
            join_type,
            on,
            left_from: Box::new(to_physical_plan(*left_from, indexes)),
            right_from: Box::new(to_physical_plan(*right_from, indexes)),
        }),
        LogicalPlan::Limit(Limit { from, limit }) => PhysicalPlan::Limit(Limit {
            from: Box::new(to_physical_plan(*from, indexes)),
            limit,
        }),
        LogicalPlan::Project(Project { from, fields }) => PhysicalPlan::Project(Project {
            from: Box::new(to_physical_plan(*from, indexes)),
            fields,
        }),
    }
}

fn columns_in_filter(expr: &Expr) -> BTreeMap<&ColumnName, FilterValue> {
    match expr {
        Expr::ColumnComparison {
            column,
            literal,
            op: Op::Equals,
        } => BTreeMap::from_iter([(&column.name, FilterValue::Eq(literal))]),
        Expr::ColumnComparison { .. } => BTreeMap::new(),
    }
}

enum FilterValue<'a> {
    Eq(&'a serde_json::Value),
}

// convert filter to physical plan. it may contain a `From` inside, in which case combine them
// into an IndexScan
fn filter_to_physical_plan(
    filter: Filter<LogicalPlan>,
    indexes: &ConstructedIndexes,
) -> PhysicalPlan {
    let Filter { from, filter } = filter;

    let inner = match from.as_ref() {
        LogicalPlan::From(From {
            table_name,
            table_alias,
        }) => {
            let filter_columns = columns_in_filter(&filter);
            if let Some((index, values)) = find_index(indexes, table_name, &filter_columns) {
                // when we have multiple comparisons in a filter,
                // remove column from filter (later)
                // for now, remove filter altogether
                // create index scan
                return PhysicalPlan::IndexScan(IndexScan {
                    index: index.clone(),
                    table_name: table_name.clone(),
                    table_alias: table_alias.clone(),
                    values,
                });
            }
            to_physical_plan(*from, indexes)
        }
        _ => to_physical_plan(*from, indexes),
    };

    PhysicalPlan::Filter(Filter {
        from: Box::new(inner),
        filter,
    })
}

fn find_index<'index>(
    indexes: &'index ConstructedIndexes,
    table_name: &'_ TableName,
    filter_columns: &BTreeMap<&'_ ColumnName, FilterValue<'_>>,
) -> Option<(&'index Index, Vec<serde_json::Value>)> {
    if let Some(indexes_for_table) = indexes.indexes.get(table_name) {
        for (index, _) in indexes_for_table {
            if let Some(values) = index
                .columns
                .iter()
                .map(|column| {
                    filter_columns.get(column).map(|val| match val {
                        FilterValue::Eq(val) => val,
                    })
                })
                .collect::<Option<Vec<_>>>()
            {
                // there's only one value here for now
                let json_value =
                    serde_json::Value::Array(values.into_iter().copied().cloned().collect());

                return Some((index, vec![json_value]));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::{
        indexes::{construct_index, ConstructedIndexes, Index},
        query::{from::raw_rows_for_table, to_physical_plan::to_physical_plan},
        types::{Expr, Filter, From, IndexScan, LogicalPlan, Op, PhysicalPlan},
    };

    #[test]
    fn test_filter_to_index_scan() {
        let artist_primary_index = Index {
            table_name: "Artist".into(),
            columns: vec!["ArtistId".into()],
        };

        let rows = raw_rows_for_table(&"Artist".into());

        let constructed_artist_primary_index = construct_index(&artist_primary_index, &rows);

        let indexes: ConstructedIndexes = ConstructedIndexes {
            indexes: BTreeMap::from_iter(
                vec![(
                    "Artist".into(),
                    vec![(
                        artist_primary_index.clone(),
                        constructed_artist_primary_index,
                    )],
                )]
                .into_iter(),
            ),
        };

        let logical_plan = LogicalPlan::Filter(Filter {
            from: Box::new(LogicalPlan::From(From {
                table_name: "Artist".into(),
                table_alias: None,
            })),
            filter: Expr::ColumnComparison {
                column: "ArtistId".into(),
                op: Op::Equals,
                literal: 1.into(),
            },
        });

        let expected = PhysicalPlan::IndexScan(IndexScan {
            table_name: "Artist".into(),
            table_alias: None,
            index: artist_primary_index,
            values: vec![json!([1])],
        });

        assert_eq!(to_physical_plan(logical_plan, &indexes), expected);
    }
}
