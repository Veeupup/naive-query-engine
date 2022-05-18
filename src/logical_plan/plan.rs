/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:09:04
 * @Email: code@tanweime.com
*/

use crate::datasource::TableRef;
use crate::logical_plan::expression::{Column, LogicalExpr};

use std::sync::Arc;

use super::schema::NaiveSchema;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection(Projection),

    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter(Filter),

    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate(Aggregate),

    /// Join two logical plans on one or more join columns
    Join(Join),

    /// Produces the first `n` tuples from its input and discards the rest.
    Limit(Limit),

    /// Produces rows from a table provider by reference or from the context
    TableScan(TableScan),
}

impl LogicalPlan {
    pub fn schema(&self) -> &NaiveSchema {
        match self {
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::TableScan(TableScan { source, .. }) => source.schema(),
        }
    }

    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        match self {
            LogicalPlan::Projection(Projection { input, .. }) => vec![input.clone()],
            LogicalPlan::Filter(Filter { input, .. }) => vec![input.clone()],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input.clone()],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left.clone(), right.clone()],
            LogicalPlan::Limit(Limit { input, .. }) => vec![input.clone()],
            LogicalPlan::TableScan(_) => vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Projection {
    /// The list of expressions
    pub exprs: Vec<LogicalExpr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: NaiveSchema,
}

#[derive(Debug, Clone)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: LogicalExpr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct TableScan {
    /// The source of the table
    pub source: TableRef,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
}

/// Aggregates its input based on a set of grouping and aggregate
/// expressions (e.g. SUM).
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<LogicalExpr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<LogicalExpr>,
    /// The schema description of the aggregate output
    pub schema: NaiveSchema,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// Join two logical plans on one or more join columns
#[derive(Debug, Clone)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// Equijoin clause expressed as pairs of (left, right) join columns
    pub on: Vec<(Column, Column)>,
    /// Join type
    pub join_type: JoinType,
    /// The output schema, containing fields from the left and right inputs
    pub schema: NaiveSchema,
}

#[derive(Debug, Clone)]

/// Produces the first `n` tuples from its input and discards the rest.
pub struct Limit {
    /// The limit
    pub n: usize,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::EmptyTable;

    use crate::error::Result;
    use crate::logical_plan::expression::*;
    

    /// Create LogicalPlan
    #[test]
    fn create_logical_plan() -> Result<()> {
        let schema = NaiveSchema::empty();
        let source = EmptyTable::try_create(schema)?;

        let scan = LogicalPlan::TableScan(TableScan {
            source,
            projection: None,
        });

        let filter_expr = LogicalExpr::BinaryExpr(BinaryExpr {
            left: Box::new(LogicalExpr::column(None, "state".to_string())),
            op: Operator::Eq,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Utf8(Some(
                "CO".to_string(),
            )))),
        });

        let _selection = LogicalPlan::Filter(Filter {
            predicate: filter_expr,
            input: Arc::new(scan),
        });

        let _projection = vec![
            LogicalExpr::column(None, "id".to_string()),
            LogicalExpr::column(None, "first_name".to_string()),
            LogicalExpr::column(None, "last_name".to_string()),
            LogicalExpr::column(None, "state".to_string()),
            LogicalExpr::column(None, "salary".to_string()),
        ];

        Ok(())
    }
}
