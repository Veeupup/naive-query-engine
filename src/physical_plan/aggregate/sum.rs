/*
 * @Author: Veeupup
 * @Date: 2022-05-20 19:09:44
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:19:45
 */

use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;

use super::AggregateOperator;
use crate::error::ErrorCode;
use crate::logical_plan::expression::ScalarValue;
use crate::logical_plan::schema::NaiveField;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;
use crate::Result;

#[derive(Debug)]
pub struct Sum {
    // TODO(veeupup): should use generic type for Int64, UInt Float64
    sum: i64,
    // physical column
    col_expr: ColumnExpr,
}

impl Sum {
    pub fn create(col_expr: ColumnExpr) -> Box<dyn AggregateOperator> {
        Box::new(Self { sum: 0, col_expr })
    }
}

impl AggregateOperator for Sum {
    fn data_field(&self) -> NaiveField {
        NaiveField::new(None, "sum(x)", DataType::Int64, true)
    }

    fn update(&mut self, data: &RecordBatch) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        match col.data_type() {
            DataType::Int64 => {
                let col = col
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap();
                for val in col.into_iter().flatten() {
                    self.sum += val;
                }
            }
            DataType::UInt64 => todo!(),
            DataType::Float64 => todo!(),
            _ => {
                return Err(ErrorCode::NotSupported(format!(
                    "Sum func for {:?} is not supported",
                    col.data_type()
                )))
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.sum)))
    }
}
