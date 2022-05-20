/*
 * @Author: ywqzzy
 * @Date: 2022-05-19
*/

use arrow::{
    array::PrimitiveArray,
    datatypes::{DataType, Float64Type, Int64Type},
    record_batch::RecordBatch,
};
use core::fmt;
use std::{sync::Arc, fmt::{Debug, Formatter}};

use super::{PhysicalExpr, PhysicalExprRef};
use crate::{
    datatype::ColumnValue,
    logical_plan::expression::UnaryOperator,
};

macro_rules! unary_op {
    ($OP:ident, $DT: expr, $COL: expr) => {{
        match $DT {
            DataType::Int64 => {
                let value = $COL
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap();
                let res: PrimitiveArray<Int64Type> = arrow::compute::kernels::arity::unary(value, |x| x.$OP());
                Ok(ColumnValue::Array(Arc::new(res)))
            }
            DataType::Float64 => {
                let value = $COL
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap();
                let res: PrimitiveArray<Float64Type> = arrow::compute::kernels::arity::unary(value, |x| x.$OP());
                Ok(ColumnValue::Array(Arc::new(res)))
            }
            _ => unimplemented!(),
        }
    }};
}

pub struct PhysicalUnaryExpr {
    expr: PhysicalExprRef,
    func: UnaryOperator,
    name: String,
    return_type: DataType
}

impl Debug for PhysicalUnaryExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("UnaryExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("expr", &self.expr)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl PhysicalUnaryExpr {
    pub fn create(expr: PhysicalExprRef, func: UnaryOperator, name: String, return_type: &DataType) -> PhysicalExprRef {
        Arc::new(Self { expr, func, name, return_type: return_type.clone() })
    }
}

impl PhysicalExpr for PhysicalUnaryExpr {
    fn evaluate(&self, input: &RecordBatch) -> crate::Result<ColumnValue> {
        let value = self.expr.evaluate(input)?;

        let data_type = value.data_type();

        let value_array = value.into_array();

        match self.func {
            UnaryOperator::Abs => unary_op!(abs, data_type, value_array),
            UnaryOperator::Sin => todo!(),
            UnaryOperator::Cos => todo!(),
            UnaryOperator::Tan => todo!(),
            UnaryOperator::Trim => todo!(),
            UnaryOperator::LTrim => todo!(),
            UnaryOperator::RTrim => todo!(),
            UnaryOperator::CharacterLength => todo!(),
            UnaryOperator::Lower => todo!(),
            UnaryOperator::Upper => todo!(),
            UnaryOperator::Repeat => todo!(),
            UnaryOperator::Replace => todo!(),
            UnaryOperator::Reverse => todo!(),
            UnaryOperator::Substr => todo!(),
        }
    }
}
