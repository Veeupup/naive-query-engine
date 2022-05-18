/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:26:59
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use crate::datasource::TableRef;
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use crate::physical_plan::PhysicalPlan;
use crate::physical_plan::PhysicalPlanRef;

#[derive(Debug, Clone)]
pub struct ScanPlan {
    source: TableRef,
    projection: Option<Vec<usize>>,
}

impl ScanPlan {
    pub fn create(source: TableRef, projection: Option<Vec<usize>>) -> PhysicalPlanRef {
        Arc::new(Self { source, projection })
    }
}

impl PhysicalPlan for ScanPlan {
    fn schema(&self) -> &NaiveSchema {
        self.source.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.source.scan(self.projection.clone())
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::{CsvConfig, CsvTable};
    use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};

    use super::*;

    #[test]
    fn test_physical_scan() -> Result<()> {
        let source = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;

        let scan_plan = ScanPlan::create(source, None);

        let result = scan_plan.execute()?;

        assert_eq!(result.len(), 1);
        let record_batch = &result[0];
        assert_eq!(record_batch.columns().len(), 4);

        let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4, 5, 6, 7, 8, 9]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
            "veeupup", "alex", "lynne", "alice", "bob", "jack", "cock", "primer",
        ]));
        let age_excepted: ArrayRef =
            Arc::new(Int64Array::from(vec![23, 20, 18, 19, 20, 21, 22, 23]));
        let score_excepted: ArrayRef = Arc::new(Float64Array::from(vec![
            60.0, 90.1, 99.99, 81.1, 82.2, 83.3, 84.4, 85.5,
        ]));

        assert_eq!(record_batch.column(0), &id_excepted);
        assert_eq!(record_batch.column(1), &name_excepted);
        assert_eq!(record_batch.column(2), &age_excepted);
        assert_eq!(record_batch.column(3), &score_excepted);

        Ok(())
    }
}
