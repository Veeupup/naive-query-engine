/*
 * @Author: Veeupup
 * @Date: 2022-05-20 14:12:40
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:24:37
 */

pub mod count;
pub mod sum;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::{expression::ScalarValue, schema::NaiveSchema};

use super::{PhysicalPlan, PhysicalPlanRef, concat_batches};

use crate::physical_plan::PhysicalExprRef;
use crate::Result;
use arrow::array::PrimitiveArray;
use arrow::datatypes::{Field, Schema, DataType, Int64Type};
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
        // output schema
        let mut aggr_ops = self.aggr_ops.lock().unwrap();
        let len = aggr_ops.len();
        let mut fields: Vec<Field> = vec![];
        for aggr_op in aggr_ops.iter() {
            fields.push(aggr_op.data_field(self.schema())?.into());
        }
        let schema = Arc::new(Schema::new(fields));

        if self.group_expr.is_empty() {
            let batches = self.input.execute()?;

            for batch in &batches {
                for i in 0..len {
                    aggr_ops.get_mut(i).unwrap().update_batch(batch)?;
                }
            }

            let mut arrays = vec![];
            for aggr_op in aggr_ops.iter() {
                let x = aggr_op.evaluate()?;
                arrays.push(x.into_array(1));
            }

            let record_batch = RecordBatch::try_new(schema.clone(), arrays)?;
            Ok(vec![record_batch])
        } else {
            // TODO(veeupup): support multi group by expr
            // such as `select sum(id) from t1 group by id % 3, age % 2`
            let batches = self.input.execute()?;
            let single_batch = concat_batches(&self.input.schema().clone().into(), &batches)?;

            let group_by_expr = &self.group_expr[0];
            
            let val = group_by_expr.evaluate(&single_batch)?.into_array();
            match val.data_type() {
                DataType::Int64 => {
                    let group_val = val.as_any().downcast_ref::<PrimitiveArray<Int64Type>>().unwrap();
                    // group val -> Vec<index>
                    // such as group by number % 3, then we will have group_idxs like
                    // 0 -> [0,3,6], 1 -> [1,2,5] ...
                    let mut group_idxs = HashMap::<i64, Vec<usize>>::new();

                    // split into different groups
                    for (idx, val) in group_val.iter().enumerate() {
                        if let Some(val) = val {
                            if let Some(idxs) = group_idxs.get_mut(&val) {
                                idxs.push(idx);
                            }else {
                                group_idxs.insert(val, vec![idx]);
                            }
                        }
                    }

                    // for each group, calculate aggregating value
                    let mut batches = vec![];
                    
                    for (_, group_idx) in &group_idxs {


                        for idx in group_idx {
                            for i in 0..len {
                                aggr_ops.get_mut(i).unwrap().update(&single_batch, *idx)?;
                            }
                        }

                        let mut arrays = vec![];
                        // let aggr_ops = self.aggr_ops.lock().unwrap();
                        for aggr_op in aggr_ops.iter() {
                            let x = aggr_op.evaluate()?;
                            arrays.push(x.into_array(1));
                        }

                        let record_batch = RecordBatch::try_new(schema.clone(), arrays)?;
                        batches.push(record_batch);

                        // for next group aggregate usage
                        for i in 0..len {
                            aggr_ops.get_mut(i).unwrap().clear_state();
                        }
                    }


                    let single_batch = concat_batches(&schema, &batches)?;
                    Ok(vec![single_batch])
                }
                DataType::UInt64 => todo!(),
                DataType::Float64 => todo!(),
                DataType::Utf8 => todo!(),
                _ => unimplemented!()
            }
        }
    }
}

pub trait AggregateOperator: Debug {
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField>;

    fn update_batch(&mut self, data: &RecordBatch) -> Result<()>;

    fn update(&mut self, data: &RecordBatch, idx: usize) -> Result<()>;

    fn evaluate(&self) -> Result<ScalarValue>;

    fn clear_state(&mut self);
}
