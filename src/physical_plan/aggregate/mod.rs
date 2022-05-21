/*
 * @Author: Veeupup
 * @Date: 2022-05-20 14:12:40
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:24:37
 */

pub mod count;
pub mod sum;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::{expression::ScalarValue, schema::NaiveSchema};

use super::{PhysicalPlan, PhysicalPlanRef};

use crate::error::ErrorCode;
use crate::physical_plan::PhysicalExprRef;
use crate::Result;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

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
        let schema = input.schema().clone();
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

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
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

            let mut fields: Vec<Field> = vec![];
            for aggr_op in aggr_ops.iter() {
                fields.push(aggr_op.data_field(self.schema())?.into());
            }

            let schema = Arc::new(Schema::new(fields));
            let record_batch = RecordBatch::try_new(schema, arrays)?;
            Ok(vec![record_batch])
        } else {
            Err(ErrorCode::NotImplemented)
        }
    }
}

pub trait AggregateOperator: Debug {
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField>;

    fn update(&mut self, data: &RecordBatch) -> Result<()>;

    fn evaluate(&self) -> Result<ScalarValue>;
}
