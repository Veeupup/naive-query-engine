/*
 * @Author: Veeupup
 * @Date: 2022-05-14 20:26:01
 * @Email: code@tanweime.com
*/

use arrow::{
    array::{BooleanArray, PrimitiveArray},
    compute::{
        and_kleene, eq_dyn, gt_dyn, gt_eq_dyn,
        kernels::arithmetic::{add, divide, modulus, multiply, subtract},
        lt_dyn, lt_eq_dyn, neq_dyn, or_kleene,
    },
    datatypes::{DataType, Float64Type, Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
use std::sync::Arc;

use super::{PhysicalExpr, PhysicalExprRef};
use crate::{datatype::ColumnValue, error::ErrorCode, logical_plan::expression::Operator};

macro_rules! compare_bin {
    ($OP:expr, $LEFT: expr, $RIGHT: expr) => {
        $OP($LEFT, $RIGHT)
            .map_err(|e| e.into())
            .map(|a| ColumnValue::Array(Arc::new(a)))
    };
}

macro_rules! binary_op {
    ($OP:expr, $LEFT_DT: expr, $RIGHT_DT: expr, $LEFT: expr, $RIGHT: expr, $SELF_OP: expr) => {{
        if $LEFT_DT == DataType::Boolean && $RIGHT_DT == DataType::Boolean {
            let left = $LEFT.as_any().downcast_ref::<BooleanArray>().unwrap();
            let right = $RIGHT.as_any().downcast_ref::<BooleanArray>().unwrap();
            let ret = $OP(left, right)?;
            Ok(ColumnValue::Array(Arc::new(ret)))
        } else {
            Err(ErrorCode::IntervalError(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                $SELF_OP, $LEFT_DT, $RIGHT_DT
            )))
        }
    }};
}

macro_rules! arithemic_op {
    ($OP:expr, $LEFT_DT: expr, $LEFT: expr, $RIGHT: expr) => {{
        match $LEFT_DT {
            DataType::Int64 => {
                let left = $LEFT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap();
                let right = $RIGHT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap();
                let x = $OP(left, right)?;
                Ok(ColumnValue::Array(Arc::new(x)))
            }
            DataType::UInt64 => {
                let left = $LEFT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<UInt64Type>>()
                    .unwrap();
                let right = $RIGHT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<UInt64Type>>()
                    .unwrap();
                let x = $OP(left, right)?;
                Ok(ColumnValue::Array(Arc::new(x)))
            }
            DataType::Float64 => {
                let left = $LEFT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap();
                let right = $RIGHT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap();
                let x = $OP(left, right)?;
                Ok(ColumnValue::Array(Arc::new(x)))
            }
            _ => unimplemented!(),
        }
    }};
}

#[derive(Debug)]
pub struct PhysicalBinaryExpr {
    left: PhysicalExprRef,
    op: Operator,
    right: PhysicalExprRef,
}

impl PhysicalBinaryExpr {
    pub fn create(left: PhysicalExprRef, op: Operator, right: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(Self { left, op, right })
    }
}

impl PhysicalExpr for PhysicalBinaryExpr {
    fn evaluate(&self, input: &RecordBatch) -> crate::Result<ColumnValue> {
        let left_value = self.left.evaluate(input)?;
        let right_value = self.right.evaluate(input)?;

        let left_data_type = left_value.data_type();
        let right_data_type = right_value.data_type();
        if left_value.data_type() != right_value.data_type() {
            return Err(ErrorCode::IntervalError(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op, left_data_type, right_data_type
            )));
        }

        // TODO(veeupup): speed up if left_value or right_value is scalar

        let left_array = left_value.into_array();
        let right_array = right_value.into_array();

        match self.op {
            Operator::Eq => compare_bin!(eq_dyn, &left_array, &right_array),
            Operator::NotEq => compare_bin!(neq_dyn, &left_array, &right_array),
            Operator::Lt => compare_bin!(lt_dyn, &left_array, &right_array),
            Operator::LtEq => compare_bin!(lt_eq_dyn, &left_array, &right_array),
            Operator::Gt => compare_bin!(gt_dyn, &left_array, &right_array),
            Operator::GtEq => compare_bin!(gt_eq_dyn, &left_array, &right_array),
            Operator::And => binary_op!(
                and_kleene,
                left_data_type,
                right_data_type,
                left_array,
                right_array,
                Operator::And
            ),
            Operator::Or => binary_op!(
                or_kleene,
                left_data_type,
                right_data_type,
                left_array,
                right_array,
                Operator::Or
            ),
            Operator::Plus => arithemic_op!(add, left_data_type, left_array, right_array),
            Operator::Minus => arithemic_op!(subtract, left_data_type, left_array, right_array),
            Operator::Multiply => arithemic_op!(multiply, left_data_type, left_array, right_array),
            Operator::Divide => arithemic_op!(divide, left_data_type, left_array, right_array),
            Operator::Modulos => arithemic_op!(modulus, left_data_type, left_array, right_array),
        }
    }
}
