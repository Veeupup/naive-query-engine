/*
 * @Author: Veeupup
 * @Date: 2022-05-20 19:09:44
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:19:45
 */

use arrow::array::Array;
use arrow::array::PrimitiveArray;
use arrow::datatypes::DataType;

use arrow::datatypes::Float64Type;
use arrow::datatypes::Int64Type;
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use ordered_float::OrderedFloat;

use super::AggregateOperator;
use crate::error::ErrorCode;
use crate::logical_plan::expression::ScalarValue;
use crate::logical_plan::schema::NaiveField;
use crate::logical_plan::schema::NaiveSchema;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;
use crate::Result;

#[derive(Debug, Clone)]
pub struct Max {
    // TODO(veeupup): should use generic type for Int64, UInt Float64
    val: OrderedFloat<f64>,
    // physical column
    col_expr: ColumnExpr,
}

impl Max {
    pub fn create(col_expr: ColumnExpr) -> Box<dyn AggregateOperator> {
        Box::new(Self {
            val: OrderedFloat::from(f64::MIN),
            col_expr,
        })
    }
}

macro_rules! update_match {
    ($COL: expr, $DT: ty, $SELF: expr) => {{
        let col = $COL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap();
        for val in col.into_iter().flatten() {
            let val = OrderedFloat::from(val as f64);
            if val > $SELF.val {
                $SELF.val = val;
            }
        }
    }};
}

macro_rules! update_value {
    ($COL: expr, $DT: ty, $IDX: expr, $SELF: expr) => {{
        let col = $COL.as_any().downcast_ref::<PrimitiveArray<$DT>>().unwrap();
        if !col.is_null($IDX) {
            let val = OrderedFloat::from(col.value($IDX) as f64);
            if val > $SELF.val {
                $SELF.val = val;
            }
        }
    }};
}

impl AggregateOperator for Max {
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField> {
        // find by name
        if let Some(name) = &self.col_expr.name {
            let field = schema.field_with_unqualified_name(name)?;
            return Ok(NaiveField::new(
                None,
                format!("max({})", field.name()).as_str(),
                DataType::Float64,
                false,
            ));
        }

        if let Some(idx) = &self.col_expr.idx {
            let field = schema.field(*idx);
            return Ok(NaiveField::new(
                None,
                format!("max({})", field.name()).as_str(),
                DataType::Float64,
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
            DataType::Int64 => update_match!(col, Int64Type, self),
            DataType::UInt64 => update_match!(col, UInt64Type, self),
            DataType::Float64 => update_match!(col, Float64Type, self),
            _ => {
                return Err(ErrorCode::NotSupported(format!(
                    "Max func for {:?} is not supported",
                    col.data_type()
                )))
            }
        }

        Ok(())
    }

    fn update(&mut self, data: &RecordBatch, idx: usize) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        match col.data_type() {
            DataType::Int64 => update_value!(col, Int64Type, idx, self),
            DataType::UInt64 => update_value!(col, UInt64Type, idx, self),
            DataType::Float64 => update_value!(col, Float64Type, idx, self),
            _ => unimplemented!(),
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.val.into())))
    }

    fn clear_state(&mut self) {
        self.val = OrderedFloat::from(f64::MIN);
    }
}
