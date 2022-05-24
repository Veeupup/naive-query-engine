/*
 * @Author: ywq
 * @Date: 2022-05-24
 */
use super::PhysicalPlan;
use super::PhysicalPlanRef;
use crate::logical_plan::plan::JoinType;
use crate::logical_plan::schema::NaiveSchema;

use crate::Result;
use arrow::array::Array;
use arrow::datatypes::SchemaRef;
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
        let outer_table = self.left.execute()?;
        let inner_table = self.right.execute()?;

        let mut batches: Vec<RecordBatch> = vec![];

        let left_rows = outer_table[0].num_rows();
        let right_rows = inner_table[0].num_rows();

        println!("left rows: {}", left_rows);
        for i in 0..left_rows {
            for j in 0..right_rows {
                for fi in 0..self.left.schema().fields().len() {
                }
                for fj in 0..self.left.schema().fields().len() {
                }
            }
        }
        Ok(batches)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}
