/*
 * @Author: GanZiheng
 * @Date: 2022-05-25
 */

use super::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PhysicalOffsetPlan {
    input: PhysicalPlanRef,
    n: usize,
}

impl PhysicalOffsetPlan {
    pub fn create(input: PhysicalPlanRef, n: usize) -> PhysicalPlanRef {
        Arc::new(Self { input, n })
    }
}

impl PhysicalPlan for PhysicalOffsetPlan {
    fn schema(&self) -> &NaiveSchema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let batches = self.input.execute()?;
        let mut n = self.n;
        let mut ret = vec![];

        for batch in &batches {
            if n == 0 {
                ret.push(batch.clone());
                continue;
            }

            if n >= batch.num_rows() {
                n -= batch.num_rows();
                continue;
            }

            let remain = batch.num_rows() - n;
            ret.push(batch.slice(n, remain));
            n = 0;
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
    fn test_physical_offset() -> Result<()> {
        let source = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;

        let scan_plan = ScanPlan::create(source, None);
        let offset_plan = PhysicalOffsetPlan::create(scan_plan, 5);

        let result = offset_plan.execute()?;

        assert_eq!(result.len(), 1);
        let record_batch = &result[0];
        assert_eq!(record_batch.columns().len(), 4);

        let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec!["jack", "cock", "primer"]));
        let age_excepted: ArrayRef = Arc::new(Int64Array::from(vec![21, 22, 23]));
        let score_excepted: ArrayRef = Arc::new(Float64Array::from(vec![83.3, 84.4, 85.5]));

        assert_eq!(record_batch.column(0), &id_excepted);
        assert_eq!(record_batch.column(1), &name_excepted);
        assert_eq!(record_batch.column(2), &age_excepted);
        assert_eq!(record_batch.column(3), &score_excepted);

        Ok(())
    }
}
