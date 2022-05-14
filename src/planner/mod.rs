/*
 * @Author: Veeupup
 * @Date: 2022-05-13 16:56:35
 * @Email: code@tanweime.com
 *
 * Planner: translate the logical plan into the physical plan.
 *
*/

use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::physical_plan::PhysicalExprRef;
use crate::physical_plan::PhysicalPlanRef;
use crate::{
    error::{ErrorCode, Result},
    logical_plan::{
        expression::{Column, LogicalExpr},
        plan::LogicalPlan,
    },
    physical_plan::{ColumnExpr, PhysicalPlan, ProjectionPlan, ScanPlan},
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
                let proj_schema = Arc::new(Schema::new(fields));
                Ok(ProjectionPlan::create(input, proj_schema, proj_expr))
            }
            LogicalPlan::Limit(_limit) => {
                todo!()
            }
            LogicalPlan::Join(_join) => {
                todo!()
            }
            LogicalPlan::Filter(_filter) => {
                todo!()
            }
            LogicalPlan::Aggregate(_aggr) => {
                todo!()
            }
        }
    }

    pub fn create_physical_expression(
        expr: &LogicalExpr,
        input: &LogicalPlan,
    ) -> Result<PhysicalExprRef> {
        match expr {
            LogicalExpr::Alias(_, _) => todo!(),
            LogicalExpr::Column(Column(name)) => {
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
            LogicalExpr::Literal(_) => todo!(),
            LogicalExpr::BinaryExpr(_) => todo!(),
            LogicalExpr::Not(_) => todo!(),
            LogicalExpr::Cast {
                expr: _,
                data_type: _,
            } => todo!(),
            LogicalExpr::ScalarFunction(_) => todo!(),
            LogicalExpr::AggregateFunction(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;

    use crate::catalog::Catalog;

    use super::*;

    #[test]
    fn test_scan_projection() -> Result<()> {
        // construct
        let mut catalog = Catalog::default();
        catalog.add_csv_table("t1", "test_data.csv")?;
        let source = catalog.get_table_df("t1")?;
        let exprs = vec![
            LogicalExpr::column("id".to_string()),
            LogicalExpr::column("name".to_string()),
            LogicalExpr::column("age".to_string()),
        ];
        let logical_plan = source.project(exprs).logical_plan();
        let physical_plan = QueryPlanner::create_physical_plan(&logical_plan)?;
        let batches = physical_plan.execute()?;

        println!("{:?}", batches);
        // test
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec!["veeupup", "alex", "lynne"]));
        let age_excepted: ArrayRef = Arc::new(Int64Array::from(vec![23, 20, 18]));

        assert_eq!(batch.column(0), &id_excepted);
        assert_eq!(batch.column(1), &name_excepted);
        assert_eq!(batch.column(2), &age_excepted);

        Ok(())
    }
}
