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

use crate::{
    error::{ErrorCode, Result},
    logical_plan::{
        expression::{Column, LogicalExpression},
        plan::LogicalPlan,
    },
    physical_plan::{ColumnExpression, PhysicalExpression, PhysicalPlan, ProjectionPlan, ScanPlan},
};

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn create_physical_plan(plan: &LogicalPlan) -> Result<Arc<dyn PhysicalPlan>> {
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
            LogicalPlan::Limit(limit) => {
                todo!()
            }
            LogicalPlan::Join(join) => {
                todo!()
            }
            LogicalPlan::Filter(filter) => {
                todo!()
            }
            LogicalPlan::Aggregate(aggr) => {
                todo!()
            }
        }
    }

    pub fn create_physical_expression(
        expr: &LogicalExpression,
        input: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalExpression>> {
        match expr {
            LogicalExpression::Alias(_, _) => todo!(),
            LogicalExpression::Column(Column(name)) => {
                for (idx, field) in input.schema().fields().iter().enumerate() {
                    if field.name() == name {
                        return ColumnExpression::try_create(None, Some(idx));
                    }
                }
                Err(ErrorCode::ColumnNotExists(format!(
                    "column `{}` not exists",
                    name
                )))
            }
            LogicalExpression::Literal(_) => todo!(),
            LogicalExpression::BinaryExpr(_) => todo!(),
            LogicalExpression::Not(_) => todo!(),
            LogicalExpression::Cast {
                expr: _,
                data_type: _,
            } => todo!(),
            LogicalExpression::ScalarFunction(_) => todo!(),
            LogicalExpression::AggregateFunction(_) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;

    use crate::{datasource::CsvTable, execution::ExecutionContext, logical_plan::plan::TableScan};

    use super::*;

    #[test]
    fn test_scan_projection() -> Result<()> {
        // construct
        let ctx = ExecutionContext::default();
        let source = ctx.csv("test_data.csv", None)?;
        let exprs = vec![
            LogicalExpression::column("id".to_string()),
            LogicalExpression::column("name".to_string()),
            LogicalExpression::column("age".to_string()),
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
