use std::collections::BTreeMap;

use crate::indexes::{self, ConstructedIndex, Index};
use crate::types::{Limit, PhysicalPlan, TableName, TableScan};

use crate::types::{Filter, From, Join, LogicalPlan, Project};

pub fn to_physical_plan(
    logical_plan: LogicalPlan,
    indexes: &BTreeMap<TableName, Vec<(Index, ConstructedIndex)>>,
) -> PhysicalPlan {
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

// convert filter to physical plan. it may contain a `From` inside, in which case combine them
// into an IndexScan
fn filter_to_physical_plan(
    filter: Filter<LogicalPlan>,
    indexes: &BTreeMap<TableName, Vec<(Index, ConstructedIndex)>>,
) -> PhysicalPlan {
    let Filter { from, filter } = filter;

    let inner = match *from {
        LogicalPlan::From(From {
            table_name,
            table_alias,
        }) => {
            // look up table_name in indexes
            dbg!(table_name);
            dbg!(table_alias);
            dbg!(&filter);
            dbg!(&indexes);
            // see
            todo!("push down filter into index scan")
        }
        other => to_physical_plan(other, indexes),
    };

    PhysicalPlan::Filter(Filter {
        from: Box::new(inner),
        filter,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::{
        indexes::{self, construct_index, ConstructedIndex, Index},
        query::{from::raw_rows_for_table, to_physical_plan::to_physical_plan},
        types::{Expr, Filter, From, IndexScan, LogicalPlan, Op, PhysicalPlan, TableName},
    };

    #[test]
    fn test_filter_to_index_scan() {
        let artist_primary_index = Index {
            table_name: "Artist".into(),
            columns: vec!["ArtistId".into()],
        };

        let rows = raw_rows_for_table(&"Artist".into());

        let constructed_artist_primary_index = construct_index(&artist_primary_index, &rows);

        let indexes: BTreeMap<TableName, Vec<(Index, ConstructedIndex)>> = BTreeMap::from_iter(
            vec![(
                "Artist".into(),
                vec![(
                    artist_primary_index.clone(),
                    constructed_artist_primary_index,
                )],
            )]
            .into_iter(),
        );

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
