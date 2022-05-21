/*
 * @Author: Veeupup
 * @Date: 2022-05-20 19:09:44
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:19:45
 */

use arrow::array::Array;
use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;

use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;

use super::AggregateOperator;
use crate::error::ErrorCode;
use crate::logical_plan::expression::ScalarValue;
use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;
use crate::Result;

#[derive(Debug, Clone)]
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
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField> {
        // find by name
        if let Some(name) = &self.col_expr.name {
            let field = schema.field_with_unqualified_name(name)?;
            return Ok(NaiveField::new(
                None,
                format!("sum({})", field.name()).as_str(),
                DataType::Int64,
                false,
            ));
        }

        if let Some(idx) = &self.col_expr.idx {
            let field = schema.field(*idx);
            return Ok(NaiveField::new(
                None,
                format!("sum({})", field.name()).as_str(),
                DataType::Int64,
                false,
            ));
        }

        Err(ErrorCode::LogicalError(
            "ColumnExpr must has name or idx".to_string(),
        ))
    }

    fn update_batch(&mut self, data: &RecordBatch) -> Result<()> {
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

    fn update(&mut self, data: &RecordBatch, idx: usize) -> Result<()>{
        let col = self.col_expr.evaluate(data)?.into_array();
        match col.data_type() {
            DataType::Int64 => {
                let col = col.as_any().downcast_ref::<PrimitiveArray<Int64Type>>().unwrap();
                if !col.is_null(idx) {
                    self.sum += col.value(idx);
                }
            },
            DataType::UInt64 => todo!(),
            DataType::Float64 => todo!(),
            _ => unimplemented!()
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.sum)))
    }

    fn clear_state(&mut self) {
        self.sum = 0;
    }
}
