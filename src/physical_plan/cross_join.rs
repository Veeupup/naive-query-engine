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
use arrow::array::Float64Array;
use arrow::array::Int64Array;
use arrow::array::PrimitiveArray;
use arrow::array::StringArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::UInt64Type;
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
        let outer_table = self.left.execute()?;
        let inner_table = self.right.execute()?;

        let mut batches: Vec<RecordBatch> = vec![];

        for outer in &outer_table {
            for inner in &inner_table {
                let mut columns: Vec<Arc<dyn Array>> = vec![];
                let left_rows = outer.num_rows();
                let right_rows = inner.num_rows();
                for i in 0..self.left.schema().fields().len() {
                    let array = outer.column(i);
                    let dt = self.left.schema().field(i).data_type();
                    match dt {
                        // TODO(ywq reafctor with macro)
                        DataType::Int64 => {
                            let mut t_vec = vec![];
                            let left_col = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int64Type>>()
                                .unwrap();
                            for _ in 0..right_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(Int64Array::from(t_vec)));
                        }
                        DataType::UInt64 => {
                            let mut t_vec = vec![];
                            let left_col = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                                .unwrap();
                            for _ in 0..right_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(UInt64Array::from(t_vec)));
                        }
                        DataType::Float64 => {
                            let mut t_vec = vec![];
                            let left_col = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Float64Type>>()
                                .unwrap();
                            for _ in 0..right_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(Float64Array::from(t_vec)));
                        }
                        DataType::Utf8 => {
                            let mut t_vec = vec![];
                            let left_col = array.as_any().downcast_ref::<StringArray>().unwrap();
                            for _ in 0..right_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(StringArray::from(t_vec)));
                        }
                        _ => unimplemented!(),
                    }
                }
                for i in 0..self.right.schema().fields().len() {
                    let array = inner.column(i);
                    let dt = self.right.schema().field(i).data_type();
                    match dt {
                        DataType::Int64 => {
                            let mut t_vec = vec![];
                            let left_col = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int64Type>>()
                                .unwrap();
                            for _ in 0..left_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(Int64Array::from(t_vec)));
                        }
                        DataType::UInt64 => {
                            let mut t_vec = vec![];
                            let left_col = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                                .unwrap();
                            for _ in 0..left_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(UInt64Array::from(t_vec)));
                        }
                        DataType::Float64 => {
                            let mut t_vec = vec![];
                            let left_col = array
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Float64Type>>()
                                .unwrap();
                            for _ in 0..left_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(Float64Array::from(t_vec)));
                        }
                        DataType::Utf8 => {
                            let mut t_vec = vec![];
                            let left_col = array.as_any().downcast_ref::<StringArray>().unwrap();
                            for _ in 0..left_rows {
                                for k in 0..left_col.len() {
                                    t_vec.push(left_col.value(k))
                                }
                            }
                            columns.push(Arc::new(StringArray::from(t_vec)));
                        }
                        _ => unimplemented!(),
                    }
                }
                // new batch
                let batch = RecordBatch::try_new(SchemaRef::from(self.schema.clone()), columns)?;
                batches.push(batch);
            }
        }
        Ok(batches)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}
