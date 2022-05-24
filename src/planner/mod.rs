/*
 * @Author: Veeupup
 * @Date: 2022-05-13 16:56:35
 * @Email: code@tanweime.com
 *
 * Planner: translate the logical plan into the physical plan.
 *
*/

use crate::logical_plan::expression::AggregateFunc;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::CrossJoin;
use crate::physical_plan::HashJoin;

use crate::physical_plan::count::Count;
use crate::physical_plan::sum::Sum;
use crate::physical_plan::PhysicalAggregatePlan;
use crate::physical_plan::PhysicalBinaryExpr;
use crate::physical_plan::PhysicalCastExpr;
use crate::physical_plan::PhysicalExprRef;
use crate::physical_plan::PhysicalLimitPlan;
use crate::physical_plan::PhysicalLiteralExpr;
use crate::physical_plan::PhysicalPlanRef;
use crate::physical_plan::PhysicalUnaryExpr;
use crate::physical_plan::SelectionPlan;
use crate::{
    error::{ErrorCode, Result},
    logical_plan::{
        expression::{Column, LogicalExpr},
        plan::LogicalPlan,
    },
    physical_plan::{ColumnExpr, ProjectionPlan, ScanPlan},
};

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn create_physical_plan(plan: &LogicalPlan) -> Result<PhysicalPlanRef> {
        match plan {
            LogicalPlan::TableScan(table_scan) => Ok(ScanPlan::create(
                table_scan.source.clone(),
                table_scan.projection.clone(),
            )),
            LogicalPlan::Projection(proj) => {
                let input = Self::create_physical_plan(&proj.input)?;
                let proj_expr = proj
                    .exprs
                    .iter()
                    .map(|expr| Self::create_physical_expression(expr, &proj.input).unwrap())
                    .collect::<Vec<_>>();
                let fields = proj
                    .exprs
                    .iter()
                    .map(|expr| expr.data_field(proj.input.as_ref()).unwrap())
                    .collect::<Vec<_>>();
                let proj_schema = NaiveSchema::new(fields);
                Ok(ProjectionPlan::create(input, proj_schema, proj_expr))
            }
            LogicalPlan::Limit(limit) => {
                let plan = Self::create_physical_plan(&limit.input)?;
                Ok(PhysicalLimitPlan::create(plan, limit.n))
            }
            LogicalPlan::Join(join) => {
                let left = Self::create_physical_plan(&join.left)?;
                let right = Self::create_physical_plan(&join.right)?;
                // We now have two join physical implementation
                // Ok(NestedLoopJoin::new(
                //     left,
                //     right,
                //     join.on.clone(),
                //     join.join_type,
                //     join.schema.clone(),
                // ))
                Ok(HashJoin::create(
                    left,
                    right,
                    join.on.clone(),
                    join.join_type,
                    join.schema.clone(),
                ))
            }
            LogicalPlan::Filter(filter) => {
                let predicate = Self::create_physical_expression(&filter.predicate, plan)?;
                let input = Self::create_physical_plan(&filter.input)?;
                Ok(SelectionPlan::create(input, predicate))
            }
            LogicalPlan::Aggregate(aggr) => {
                let mut group_exprs = vec![];
                for group_expr in &aggr.group_expr {
                    group_exprs.push(Self::create_physical_expression(group_expr, &aggr.input)?);
                }

                let mut aggr_ops = vec![];
                for aggr_expr in &aggr.aggr_expr {
                    let aggr_op = match aggr_expr.fun {
                        AggregateFunc::Count => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Count::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                        AggregateFunc::Sum => {
                            let expr =
                                Self::create_physical_expression(&aggr_expr.args, &aggr.input)?;
                            let col_expr = expr.as_any().downcast_ref::<ColumnExpr>();
                            if let Some(col_expr) = col_expr {
                                Sum::create(col_expr.clone())
                            } else {
                                return Err(ErrorCode::PlanError(
                                    "Aggregate Func should have a column in it".to_string(),
                                ));
                            }
                        }
                        AggregateFunc::Avg => todo!(),
                        AggregateFunc::Min => todo!(),
                        AggregateFunc::Max => todo!(),
                    };
                    aggr_ops.push(aggr_op);
                }

                let input = Self::create_physical_plan(&aggr.input)?;
                Ok(PhysicalAggregatePlan::create(group_exprs, aggr_ops, input))
            }
            LogicalPlan::CrossJoin(join) => {
                let left = Self::create_physical_plan(&join.left)?;
                let right = Self::create_physical_plan(&join.right)?;
                Ok(CrossJoin::create(
                    left,
                    right,
                    join.join_type,
                    join.schema.clone(),
                ))
            }
        }
    }

    pub fn create_physical_expression(
        expr: &LogicalExpr,
        input: &LogicalPlan,
    ) -> Result<PhysicalExprRef> {
        match expr {
            LogicalExpr::Alias(_, _) => todo!(),
            LogicalExpr::Column(Column { name, .. }) => {
                for (idx, field) in input.schema().fields().iter().enumerate() {
                    if field.name() == name {
                        return ColumnExpr::try_create(None, Some(idx));
                    }
                }
                Err(ErrorCode::ColumnNotExists(format!(
                    "column `{}` not exists",
                    name
                )))
            }
            LogicalExpr::Literal(scalar_val) => Ok(PhysicalLiteralExpr::create(scalar_val.clone())),
            LogicalExpr::BinaryExpr(bin_expr) => {
                let left = Self::create_physical_expression(bin_expr.left.as_ref(), input)?;
                let right = Self::create_physical_expression(bin_expr.right.as_ref(), input)?;
                let phy_bin_expr = PhysicalBinaryExpr::create(left, bin_expr.op.clone(), right);
                Ok(phy_bin_expr)
            }
            LogicalExpr::UnaryExpr(scalar_expr) => {
                let expr = Self::create_physical_expression(scalar_expr.arg.as_ref(), input)?;
                let phy_scalar_expr = PhysicalUnaryExpr::create(
                    expr,
                    scalar_expr.func.clone(),
                    "todo".to_string(),
                    &arrow::datatypes::DataType::Int32,
                );
                Ok(phy_scalar_expr)
            }
            LogicalExpr::Not(_) => todo!(),
            LogicalExpr::Cast(cast_expr) => {
                let expr = Self::create_physical_expression(cast_expr.expr.as_ref(), input)?;
                let phy_cast_expr = PhysicalCastExpr::create(expr, &cast_expr.data_type);
                Ok(phy_cast_expr)
            }
            LogicalExpr::AggregateFunction(_) => todo!(),
            LogicalExpr::Wildcard => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;
    use std::sync::Arc;

    use crate::catalog::Catalog;
    use crate::CsvConfig;

    use super::*;

    #[test]
    fn test_scan_projection() -> Result<()> {
        // construct
        let mut catalog = Catalog::default();
        catalog.add_csv_table("t1", "data/test_data.csv", CsvConfig::default())?;
        let source = catalog.get_table_df("t1")?;
        let exprs = vec![
            LogicalExpr::column(None, "id".to_string()),
            LogicalExpr::column(None, "name".to_string()),
            LogicalExpr::column(None, "age".to_string()),
        ];
        let logical_plan = source.project(exprs)?.logical_plan();
        let physical_plan = QueryPlanner::create_physical_plan(&logical_plan)?;
        let batches = physical_plan.execute()?;

        // test
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4, 5, 6, 7, 8, 9]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
            "veeupup", "alex", "lynne", "alice", "bob", "jack", "cock", "primer",
        ]));
        let age_excepted: ArrayRef =
            Arc::new(Int64Array::from(vec![23, 20, 18, 19, 20, 21, 22, 23]));

        assert_eq!(batch.column(0), &id_excepted);
        assert_eq!(batch.column(1), &name_excepted);
        assert_eq!(batch.column(2), &age_excepted);

        Ok(())
    }
}
