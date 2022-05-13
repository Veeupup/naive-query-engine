/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:54:33
 * @Email: code@tanweime.com
*/

use std::iter::Iterator;
use std::sync::Arc;

use super::{expression::PhysicalExpression, plan::PhysicalPlan};
use crate::error::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

#[derive(Debug, Clone)]
pub struct ProjectionPlan {
    input: Arc<dyn PhysicalPlan>,
    schema: SchemaRef,
    expr: Vec<Arc<dyn PhysicalExpression>>,
}

impl ProjectionPlan {
    pub fn create(
        input: Arc<dyn PhysicalPlan>,
        schema: SchemaRef,
        expr: Vec<Arc<dyn PhysicalExpression>>,
    ) -> Arc<dyn PhysicalPlan> {
        Arc::new(Self {
            input,
            schema,
            expr,
        })
    }
}

impl PhysicalPlan for ProjectionPlan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;
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
                    .map(|column| column.into_array())
                    .collect::<Vec<_>>();
                // TODO(veeupup): remove unwrap
                RecordBatch::try_new(self.schema.clone(), columns).unwrap()
            })
            .collect::<Vec<_>>();
        Ok(batches)
    }

    fn children(&self) -> Result<Vec<Arc<dyn PhysicalPlan>>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{CsvConfig, CsvTable, TableSource};
    use crate::physical_plan::expression::{ColumnExpression, PhysicalExpression};
    use crate::physical_plan::scan::ScanPlan;
    use arrow::{
        array::{Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    #[test]
    fn test_projection() -> Result<()> {
        let source = CsvTable::try_create("test_data.csv", CsvConfig::default())?;
        let schema = Arc::new(Schema::new(vec![
            source.schema().field(0).clone(),
            source.schema().field(1).clone(),
        ]));
        let scan_plan = ScanPlan::create(source, None);

        let expr = vec![
            ColumnExpression::try_create(None, Some(0))?,
            ColumnExpression::try_create(Some("name".to_string()), None)?,
        ];
        let proj_plan = ProjectionPlan::create(scan_plan, schema, expr);

        let res = proj_plan.execute()?;

        assert_eq!(res.len(), 1);
        let batch = &res[0];

        let id_excepted: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2, 4]));
        let name_excepted: Arc<dyn Array> =
            Arc::new(StringArray::from(vec!["veeupup", "alex", "lynne"]));

        assert_eq!(batch.column(0), &id_excepted);
        assert_eq!(batch.column(1), &name_excepted);

        Ok(())
    }
}
