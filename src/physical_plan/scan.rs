/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:26:59
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use crate::datasource::TableSource;
use crate::error::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use super::plan::PhysicalPlan;

#[derive(Debug, Clone)]
pub struct ScanPlan {
    source: Arc<dyn TableSource>,
    projection: Option<Vec<usize>>,
}

impl ScanPlan {
    pub fn create(
        source: Arc<dyn TableSource>,
        projection: Option<Vec<usize>>,
    ) -> Arc<dyn PhysicalPlan> {
        Arc::new(Self { source, projection })
    }
}

impl PhysicalPlan for ScanPlan {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.source.scan(self.projection.clone())
    }

    fn children(&self) -> Result<Vec<Arc<dyn PhysicalPlan>>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::{CsvConfig, CsvTable, TableSource};
    use arrow::{
        array::{Array, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    #[test]
    fn test_physical_scan() -> Result<()> {
        let source = CsvTable::try_create("test_schema.txt", CsvConfig::default())?;

        let scan_plan = ScanPlan::create(source, None);

        let result = scan_plan.execute()?;

        assert_eq!(result.len(), 1);
        let record_batch = &result[0];
        assert_eq!(record_batch.columns().len(), 4);

        let id_excepted: Arc<dyn Array> = Arc::new(Int64Array::from(vec![1, 2, 4]));
        let name_excepted: Arc<dyn Array> =
            Arc::new(StringArray::from(vec!["veeupup", "alex", "lynne"]));
        let age_excepted: Arc<dyn Array> = Arc::new(Int64Array::from(vec![23, 20, 18]));
        let score_excepted: Arc<dyn Array> = Arc::new(Float64Array::from(vec![60.0, 90.1, 99.99]));

        assert_eq!(record_batch.column(0), &id_excepted);
        assert_eq!(record_batch.column(1), &name_excepted);
        assert_eq!(record_batch.column(2), &age_excepted);
        assert_eq!(record_batch.column(3), &score_excepted);

        Ok(())
    }
}
