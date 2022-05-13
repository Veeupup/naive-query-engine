/*
 * @Author: Veeupup
 * @Date: 2022-05-12 22:52:47
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};

use super::{Aggregate, Filter, LogicalPlan, Projection};
use crate::logical_plan::expression::LogicalExpression;

pub struct DataFrame {
    pub plan: Arc<LogicalPlan>,
}

impl DataFrame {
    pub fn project(self, exprs: Vec<LogicalExpression>) -> Self {
        let fields = exprs
            .iter()
            .map(|expr| expr.data_field(self.plan.as_ref()).unwrap())
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        Self {
            plan: Arc::new(LogicalPlan::Projection(Projection {
                input: self.plan,
                exprs,
                schema,
            })),
        }
    }

    pub fn filter(self, expr: LogicalExpression) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::Filter(Filter {
                input: self.plan,
                predicate: expr,
            })),
        }
    }

    pub fn aggregate(
        self,
        group_expr: Vec<LogicalExpression>,
        aggr_expr: Vec<LogicalExpression>,
    ) -> Self {
        let mut group_fields = group_expr
            .iter()
            .map(|expr| expr.data_field(self.plan.as_ref()).unwrap())
            .collect::<Vec<_>>();
        let mut aggr_fields = aggr_expr
            .iter()
            .map(|expr| expr.data_field(self.plan.as_ref()).unwrap())
            .collect::<Vec<_>>();
        group_fields.append(&mut aggr_fields);
        let schema = Arc::new(Schema::new(group_fields));
        Self {
            plan: Arc::new(LogicalPlan::Aggregate(Aggregate {
                input: self.plan,
                group_expr,
                aggr_expr,
                schema,
            })),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }

    pub fn logical_plan(&self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::datasource::EmptyTable;
    use crate::datasource::TableSource;
    use crate::error::Result;
    use crate::execution::ExecutionContext;
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
        let ctx = ExecutionContext::default();

        let plan = ctx
            .empty(schema)?
            .filter(LogicalExpression::BinaryExpr(BinaryExpr {
                left: Box::new(LogicalExpression::Column(Column {
                    name: "state".to_string(),
                })),
                op: Operator::Eq,
                right: Box::new(LogicalExpression::Literal(ScalarValue::Utf8(Some(
                    "CO".to_string(),
                )))),
            }))
            .project(vec![
                LogicalExpression::Column(Column {
                    name: "id".to_string(),
                }),
                LogicalExpression::Column(Column {
                    name: "first_name".to_string(),
                }),
                LogicalExpression::Column(Column {
                    name: "last_name".to_string(),
                }),
                LogicalExpression::Column(Column {
                    name: "state".to_string(),
                }),
                LogicalExpression::Column(Column {
                    name: "salary".to_string(),
                }),
            ]);

        Ok(())
    }
}
