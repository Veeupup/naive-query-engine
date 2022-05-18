/*
 * @Author: Veeupup
 * @Date: 2022-05-12 22:52:47
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use crate::logical_plan::expression::LogicalExpr;
use crate::logical_plan::plan::{Aggregate, Filter, LogicalPlan, Projection};

use super::expression::Column;
use super::plan::{Join, JoinType, Limit};
use super::schema::NaiveSchema;
use crate::error::{ErrorCode, Result};

#[derive(Clone)]
pub struct DataFrame {
    pub plan: LogicalPlan,
}

impl DataFrame {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn project(self, exprs: Vec<LogicalExpr>) -> Result<Self> {
        // TODO(veeupup): Ambiguous reference of field
        let mut fields = vec![];
        let mut new_exprs = vec![];
        for expr in &exprs {
            match expr {
                LogicalExpr::Wildcard => {
                    let schema = self.plan.schema();
                    schema.fields().iter().for_each(|field| {
                        fields.push(field.clone());
                        new_exprs.push(LogicalExpr::column(None, field.name().clone()));
                    });
                }
                _ => {
                    fields.push(expr.data_field(&self.plan)?);
                    new_exprs.push(expr.clone());
                }
            }
        }
        let schema = NaiveSchema::new(fields);
        Ok(Self {
            plan: LogicalPlan::Projection(Projection {
                input: Arc::new(self.plan),
                exprs: new_exprs,
                schema,
            }),
        })
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
        let schema = NaiveSchema::new(group_fields);
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
        right: &LogicalPlan,
        join_type: JoinType,
        join_keys: (Vec<Column>, Vec<Column>),
    ) -> Result<DataFrame> {
        if join_keys.0.len() != join_keys.1.len() {
            return Err(ErrorCode::PlanError(
                "left_keys length must be equal to right_keys length".to_string(),
            ));
        }

        let (left_keys, right_keys) = join_keys;
        let on: Vec<(_, _)> = left_keys.into_iter().zip(right_keys.into_iter()).collect();

        let left_schema = self.plan.schema();
        let join_schema = left_schema.join(right.schema());

        Ok(Self::new(LogicalPlan::Join(Join {
            left: Arc::new(self.plan.clone()),
            right: Arc::new(right.clone()),
            on,
            join_type,
            schema: join_schema,
        })))
    }

    pub fn schema(&self) -> &NaiveSchema {
        self.plan.schema()
    }

    pub fn logical_plan(self) -> LogicalPlan {
        self.plan
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{catalog::Catalog, logical_plan::schema::NaiveField};

    use crate::error::Result;
    use crate::logical_plan::expression::*;
    use arrow::datatypes::DataType;

    #[test]
    fn create_logical_plan() -> Result<()> {
        let schema = NaiveSchema::new(vec![
            NaiveField::new(None, "state", DataType::Int64, true),
            NaiveField::new(None, "id", DataType::Int64, true),
            NaiveField::new(None, "first_name", DataType::Utf8, true),
            NaiveField::new(None, "last_name", DataType::Utf8, true),
            NaiveField::new(None, "salary", DataType::Int64, true),
        ]);
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
