/*
 * @Author: Veeupup
 * @Date: 2022-05-18 16:00:13
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-18 17:58:32
 */
use super::PhysicalPlan;
use super::PhysicalPlanRef;
use crate::error::ErrorCode;
use crate::logical_plan::expression::Column;
use crate::logical_plan::plan::JoinType;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;

use crate::Result;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::array::StringArray;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::datatypes::Float64Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;

#[derive(Debug, Clone)]
pub struct NestedLoopJoin {
    left: PhysicalPlanRef,
    right: PhysicalPlanRef,
    on: Vec<(Column, Column)>,
    join_type: JoinType,
    schema: NaiveSchema,
}

impl NestedLoopJoin {
    pub fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        on: Vec<(Column, Column)>,
        join_type: JoinType,
        schema: NaiveSchema,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            left,
            right,
            on,
            join_type,
            schema,
        })
    }
}

macro_rules! join_match {
    ($DATATYPE: ty, $LEFT_COL: expr, $RIGHT_COL: expr, $OUTER_POS: expr, $INNER_POS: expr) => {{
        let left_col = $LEFT_COL
            .as_any()
            .downcast_ref::<PrimitiveArray<$DATATYPE>>()
            .unwrap();
        let right_col = $RIGHT_COL
            .as_any()
            .downcast_ref::<PrimitiveArray<$DATATYPE>>()
            .unwrap();

        for (x_pos, x) in left_col.iter().enumerate() {
            for (y_pos, y) in right_col.iter().enumerate() {
                match (x, y) {
                    (Some(x), Some(y)) => {
                        if x == y {
                            // equal and we should
                            $OUTER_POS.append_value(x_pos as i64);
                            $INNER_POS.append_value(y_pos as i64);
                        }
                    }
                    _ => {}
                }
            }
        }
    }};
}

impl PhysicalPlan for NestedLoopJoin {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let outer_table = self.left.execute()?;
        let inner_table = self.right.execute()?;

        let mut batches: Vec<RecordBatch> = vec![];
        // TODO(veeupup): support multi on conditions
        // Using for loop to combine different conditions
        if self.on.len() == 0 {
            return Err(ErrorCode::PlanError(
                "Inner Join on Conditions can't not be empty".to_string(),
            ));
        }

        let (left_col, right_col) = &self.on[0];
        let left_col = ColumnExpr::try_create(Some(left_col.name.clone()), None)?;
        let right_col = ColumnExpr::try_create(Some(right_col.name.clone()), None)?;

        for outer in &outer_table {
            let left_col = left_col.evaluate(outer)?.into_array();

            let dt = left_col.data_type();
            for inner in &inner_table {
                let right_col = right_col.evaluate(inner)?.into_array();

                // check if ok
                if left_col.data_type() != right_col.data_type() {
                    return Err(ErrorCode::PlanError(format!(
                        "Join on left and right data type should be same: left: {:?}, right: {:?}",
                        left_col.data_type(),
                        right_col.data_type()
                    )));
                }

                let mut outer_pos = Int64Builder::new(left_col.len());
                let mut inner_pos = Int64Builder::new(right_col.len());
                match dt {
                    DataType::Int64 => {
                        join_match!(Int64Type, left_col, right_col, outer_pos, inner_pos)
                    }
                    DataType::UInt64 => {
                        join_match!(UInt64Type, left_col, right_col, outer_pos, inner_pos)
                    }
                    DataType::Float64 => {
                        join_match!(Float64Type, left_col, right_col, outer_pos, inner_pos)
                    }
                    DataType::Utf8 => {
                        let left_col = left_col.as_any().downcast_ref::<StringArray>().unwrap();
                        let right_col = right_col.as_any().downcast_ref::<StringArray>().unwrap();

                        for (x_pos, x) in left_col.iter().enumerate() {
                            for (y_pos, y) in right_col.iter().enumerate() {
                                match (x, y) {
                                    (Some(x), Some(y)) => {
                                        if x == y {
                                            // equal and we should
                                            outer_pos.append_value(x_pos as i64);
                                            inner_pos.append_value(y_pos as i64);
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    _ => unimplemented!(),
                }
                let mut columns = vec![];

                let outer_pos = outer_pos.finish();
                let inner_pos = inner_pos.finish();

                // add left columns
                for i in 0..self.left.schema().fields().len() {
                    let array = outer.column(i);
                    columns.push(compute::take(array.as_ref(), &outer_pos, None)?);
                }

                // add right columns
                for i in 0..self.right.schema().fields().len() {
                    let array = inner.column(i);
                    columns.push(compute::take(array.as_ref(), &inner_pos, None)?);
                }

                let batch = RecordBatch::try_new(self.schema.clone().into(), columns)?;
                batches.push(batch);
            }
        }

        return Ok(batches);
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}
