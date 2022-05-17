/*
 * @Author: Veeupup
 * @Date: 2022-05-12 22:52:47
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};

use crate::logical_plan::expression::LogicalExpr;
use crate::logical_plan::plan::{Aggregate, Filter, LogicalPlan, Projection};

use super::expression::Column;
use super::plan::{JoinType, Limit};
use crate::error::Result;

#[derive(Clone)]
pub struct DataFrame {
    pub plan: LogicalPlan,
}

impl DataFrame {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn project(self, exprs: Vec<LogicalExpr>) -> Self {
        let fields = exprs
            .iter()
            .map(|expr| expr.data_field(&self.plan).unwrap())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        Self {
            plan: LogicalPlan::Projection(Projection {
                input: Arc::new(self.plan),
                exprs,
                schema,
            }),
        }
    }

    pub fn filter(self, expr: LogicalExpr) -> Self {
        Self {
            plan: LogicalPlan::Filter(Filter {
                input: Arc::new(self.plan),
                predicate: expr,
            }),
        }
    }

    pub fn aggregate(self, group_expr: Vec<LogicalExpr>, aggr_expr: Vec<LogicalExpr>) -> Self {
        let mut group_fields = group_expr
            .iter()
            .map(|expr| expr.data_field(&self.plan).unwrap())
            .collect::<Vec<_>>();
        let mut aggr_fields = aggr_expr
            .iter()
            .map(|expr| expr.data_field(&self.plan).unwrap())
            .collect::<Vec<_>>();
        group_fields.append(&mut aggr_fields);
        let schema = Arc::new(Schema::new(group_fields));
        Self {
            plan: LogicalPlan::Aggregate(Aggregate {
                input: Arc::new(self.plan),
                group_expr,
                aggr_expr,
                schema,
            }),
        }
    }

    pub fn limit(self, n: usize) -> DataFrame {
        Self {
            plan: LogicalPlan::Limit(Limit {
                input: Arc::new(self.plan),
                n,
            }),
        }
    }

    pub fn join(
        &self,
        _right: &LogicalPlan,
        _join_type: JoinType,
        _join_keys: (Vec<Column>, Vec<Column>),
    ) -> Result<DataFrame> {
        todo!()
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub fn logical_plan(self) -> LogicalPlan {
        self.plan
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::catalog::Catalog;

    use crate::error::Result;
    use crate::logical_plan::expression::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn create_logical_plan() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("state", DataType::Int64, true),
            Field::new("id", DataType::Int64, true),
            Field::new("first_name", DataType::Utf8, true),
            Field::new("last_name", DataType::Utf8, true),
            Field::new("salary", DataType::Int64, true),
        ]));
        let mut catalog = Catalog::default();
        catalog.add_empty_table("empty", schema)?;

        let _plan = catalog
            .get_table_df("empty")?
            .filter(LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(LogicalExpr::column(None, "state".to_string())),
                op: Operator::Eq,
                right: Box::new(LogicalExpr::Literal(ScalarValue::Utf8(Some(
                    "CO".to_string(),
                )))),
            }))
            .project(vec![
                LogicalExpr::column(None, "id".to_string()),
                LogicalExpr::column(None, "first_name".to_string()),
                LogicalExpr::column(None, "last_name".to_string()),
                LogicalExpr::column(None, "state".to_string()),
                LogicalExpr::column(None, "salary".to_string()),
            ]);

        Ok(())
    }
}
