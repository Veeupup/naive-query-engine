/*
 * @Author: ywqzzy
 * @Date: 2022-05-19
*/

use arrow::{
    array::PrimitiveArray,
    datatypes::{DataType, Float64Type, Int64Type},
    record_batch::RecordBatch,
};
use std::sync::Arc;

use super::{PhysicalExpr, PhysicalExprRef};
use crate::{
    datatype::ColumnValue,
    logical_plan::expression::ScalarFunc,
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

#[derive(Debug)]
pub struct PhysicalScalarExpr {
    expr: PhysicalExprRef,
    func: ScalarFunc,
}

impl PhysicalScalarExpr {
    pub fn new(expr: PhysicalExprRef, func: ScalarFunc) -> PhysicalExprRef {
        Arc::new(Self { expr, func })
    }
}

impl PhysicalExpr for PhysicalScalarExpr {
    fn evaluate(&self, input: &RecordBatch) -> crate::Result<ColumnValue> {
        let value = self.expr.evaluate(input)?;

        let data_type = value.data_type();

        let value_array = value.into_array();

        match self.func {
            ScalarFunc::Abs => unary_op!(abs, data_type, value_array),
            ScalarFunc::Add => todo!(),
            ScalarFunc::Sub => todo!(),
        }
    }
}
