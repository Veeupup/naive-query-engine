/*
 * @Author: Veeupup
 * @Date: 2022-05-20 14:12:40
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:15:17
 */

pub mod count;
pub mod sum;

use std::sync::{Arc, Mutex};
use std::fmt::Debug;



use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::{schema::NaiveSchema, expression::ScalarValue};

use super::{PhysicalPlanRef, PhysicalPlan};

use arrow::record_batch::RecordBatch;
use crate::physical_plan::PhysicalExprRef;
use crate::Result;
use crate::error::ErrorCode;

#[derive(Debug)]
pub struct PhysicalAggregatePlan {
    pub group_expr: Vec<PhysicalExprRef>,
    pub aggr_ops: Mutex<Vec<Box<dyn AggregateOperator>>>,
    pub input: PhysicalPlanRef,
    pub schema: NaiveSchema,
}

impl PhysicalAggregatePlan {
    pub fn create(
        group_expr: Vec<PhysicalExprRef>,
        aggr_ops: Vec<Box<dyn AggregateOperator>>,
        input: PhysicalPlanRef,
    ) -> PhysicalPlanRef {
        let mut fields = vec![];
        for aggr_op in aggr_ops.iter() {
            fields.push(aggr_op.data_field());
        }

        let schema = NaiveSchema::new(fields);
        Arc::new(Self {
            group_expr,
            aggr_ops: Mutex::new(aggr_ops),
            input,
            schema,
        })
    }
}

impl PhysicalPlan for PhysicalAggregatePlan {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn children(&self) -> crate::Result<Vec<PhysicalPlanRef>> {
        todo!()
    }

    fn execute(&self) -> crate::Result<Vec<RecordBatch>> {
        if self.group_expr.is_empty() {
            let batches = self.input.execute()?;

            let len = self.aggr_ops.lock().unwrap().len();
            for batch in &batches {
                let mut aggr_op = self.aggr_ops.lock().unwrap();
                for i in 0..len {
                    aggr_op.get_mut(i).unwrap().update(batch)?;
                }
            }

            let mut arrays = vec![];
            let aggr_ops = self.aggr_ops.lock().unwrap();
            for aggr_op in aggr_ops.iter() {
                let x = aggr_op.evaluate()?;
                arrays.push(x.into_array(1));
            }

            let schema = Arc::new(self.schema.clone().into());
            let record_batch = RecordBatch::try_new(schema, arrays)?;
            Ok(vec![record_batch])
        }else {
            Err(ErrorCode::NotImplemented)
        }
    }
}

pub trait AggregateOperator: Debug {
    fn data_field(&self) -> NaiveField;

    fn update(&mut self, data: &RecordBatch) -> Result<()>; 

    fn evaluate(&self) -> Result<ScalarValue>;
}
