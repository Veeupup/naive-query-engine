/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:54:33
 * @Email: code@tanweime.com
*/

use std::iter::Iterator;
use std::sync::Arc;

use super::plan::PhysicalPlan;
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::PhysicalExprRef;
use crate::physical_plan::PhysicalPlanRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
#[derive(Debug, Clone)]
pub struct ProjectionPlan {
    input: PhysicalPlanRef,
    schema: NaiveSchema,
    expr: Vec<PhysicalExprRef>,
}

impl ProjectionPlan {
    pub fn create(
        input: PhysicalPlanRef,
        schema: NaiveSchema,
        expr: Vec<PhysicalExprRef>,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            input,
            schema,
            expr,
        })
    }
}

impl PhysicalPlan for ProjectionPlan {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;

        // when aggragating, we just output what input does
        if self.schema.fields().is_empty() {
            Ok(input)
        } else {
            let batches = input
                .iter()
                .map(|batch| {
                    let columns = self
                        .expr
                        .iter()
                        // TODO(veeupup): remove unwrap
                        .map(|expr| expr.evaluate(batch).unwrap())
                        .collect::<Vec<_>>();
                    let columns = columns
                        .iter()
                        .map(|column| column.clone().into_array())
                        .collect::<Vec<_>>();
                    // TODO(veeupup): remove unwrap
                    // let projection_schema = self.schema.into();
                    RecordBatch::try_new(SchemaRef::from(self.schema.clone()), columns).unwrap()
                })
                .collect::<Vec<_>>();
            Ok(batches)
        }
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{CsvConfig, CsvTable};
    use crate::logical_plan::expression::{Operator, ScalarValue};
    use crate::physical_plan::expression::ColumnExpr;
    use crate::physical_plan::scan::ScanPlan;
    use crate::physical_plan::{PhysicalBinaryExpr, PhysicalLiteralExpr};
    use arrow::array::{ArrayRef, Int64Array, StringArray};

    #[test]
    fn test_projection() -> Result<()> {
        let source = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;
        let schema = NaiveSchema::new(vec![
            source.schema().field(0).clone(),
            source.schema().field(1).clone(),
        ]);
        let scan_plan = ScanPlan::create(source, None);
        let add_expr = PhysicalBinaryExpr::create(
            ColumnExpr::try_create(Some("id".to_string()), None)?,
            Operator::Plus,
            PhysicalLiteralExpr::create(ScalarValue::Int64(Some(1))),
        );
        let expr = vec![
            // ColumnExpr::try_create(None, Some(0))?,
            add_expr,
            ColumnExpr::try_create(Some("name".to_string()), None)?,
        ];
        let proj_plan = ProjectionPlan::create(scan_plan, schema, expr);

        let res = proj_plan.execute()?;

        assert_eq!(res.len(), 1);
        let batch = &res[0];

        // let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4, 5, 6, 7, 8, 9]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
            "veeupup", "alex", "lynne", "alice", "bob", "jack", "cock", "primer",
        ]));
        let id_excepted2: ArrayRef = Arc::new(Int64Array::from(vec![2, 3, 5, 6, 7, 8, 9, 10]));
        assert_eq!(batch.column(0), &id_excepted2);
        assert_eq!(batch.column(1), &name_excepted);

        Ok(())
    }
}
