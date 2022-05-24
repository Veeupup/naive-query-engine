/*
 * @Author: ywq
 * @Date: 2022-05-24
 */
use super::PhysicalPlan;
use super::PhysicalPlanRef;
use crate::logical_plan::plan::JoinType;
use crate::logical_plan::schema::NaiveSchema;

use crate::Result;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug)]
pub struct CrossJoin {
    left: PhysicalPlanRef,
    right: PhysicalPlanRef,
    #[allow(unused)]
    join_type: JoinType,
    schema: NaiveSchema,
}

impl CrossJoin {
    #[allow(unused)]
    pub fn create(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        join_type: JoinType,
        schema: NaiveSchema,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            left,
            right,
            join_type,
            schema,
        })
    }
}

impl PhysicalPlan for CrossJoin {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        // TODO(ywq)
        let _outer_table = self.left.execute()?;
        let _inner_table = self.right.execute()?;

        let batches: Vec<RecordBatch> = vec![];

        // let left_rows = outer_table[0].num_rows();
        // let right_rows = inner_table[0].num_rows();
        // let mut columns = vec![];
        // for outer in &outer_table {
        //     let data_vec = vec![];
        //     for i in 0..self.left.schema().fields().len() {
        //         let array = outer.column(i);
        //         for j in 0..right_rows {
        //             // push to vector
        //         }
        //     }
        // }

        // for inner in &inner_table {
        //     let data_vec = vec![];
        //     for i in 0..self.right.schema().fields().len() {
        //         let array = inner.column(i);
        //         for j in 0..left_rows {
        //             // push to vector
        //         }
        //     }
        //     // data_vec to column
        //     // push column into columns
        // }
        Ok(batches)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}
