/*
 * @Author: Veeupup
 * @Date: 2022-05-17 11:27:29
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-18 14:45:03
 */

use super::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalLimitPlan {
    input: PhysicalPlanRef,
    n: usize,
}

impl PhysicalLimitPlan {
    pub fn create(input: PhysicalPlanRef, n: usize) -> PhysicalPlanRef {
        Arc::new(Self { input, n })
    }
}

impl PhysicalPlan for PhysicalLimitPlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let batches = self.input.execute()?;
        let mut n = self.n;
        let mut ret = vec![];
        for batch in &batches {
            if n == 0 {
                break;
            }
            if batch.num_rows() <= n {
                ret.push(batch.clone());
                n -= batch.num_rows();
            } else {
                ret.push(batch.slice(0, n));
                n = 0;
            };
        }
        Ok(ret)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datasource::{CsvConfig, CsvTable},
        physical_plan::ScanPlan,
    };
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};

    use super::*;

    #[test]
    fn test_physical_scan() -> Result<()> {
        let source = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;

        let scan_plan = ScanPlan::create(source, None);
        let limit_plan = PhysicalLimitPlan::create(scan_plan, 2);

        let result = limit_plan.execute()?;

        assert_eq!(result.len(), 1);
        let record_batch = &result[0];
        assert_eq!(record_batch.columns().len(), 4);

        let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec!["veeupup", "alex"]));
        let age_excepted: ArrayRef = Arc::new(Int64Array::from(vec![23, 20]));
        let score_excepted: ArrayRef = Arc::new(Float64Array::from(vec![60.0, 90.1]));

        assert_eq!(record_batch.column(0), &id_excepted);
        assert_eq!(record_batch.column(1), &name_excepted);
        assert_eq!(record_batch.column(2), &age_excepted);
        assert_eq!(record_batch.column(3), &score_excepted);

        Ok(())
    }
}
