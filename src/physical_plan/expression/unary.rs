/*
 * @Author: ywqzzy
 * @Date: 2022-05-19
*/
use arrow::{
    array::PrimitiveArray,
    datatypes::{DataType, Float32Type, Float64Type},
    record_batch::RecordBatch,
};
use core::fmt;
use std::any::Any;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use super::{PhysicalExpr, PhysicalExprRef};
use crate::{datatype::ColumnValue, logical_plan::expression::UnaryOperator};

macro_rules! unary_arith_op {
    ($OP:ident, $DT: expr, $COL: expr) => {{
        match $DT {
            DataType::Float64 => {
                let value = $COL
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap();
                let res: PrimitiveArray<Float64Type> =
                    arrow::compute::kernels::arity::unary(value, |x| x.$OP());
                Ok(ColumnValue::Array(Arc::new(res)))
            }
            DataType::Float32 => {
                let value = $COL
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float32Type>>()
                    .unwrap();
                let res: PrimitiveArray<Float32Type> =
                    arrow::compute::kernels::arity::unary(value, |x| x.$OP());
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
    return_type: DataType,
}

impl Debug for PhysicalUnaryExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("UnaryExpr")
            .field("func", &"<FUNC>")
            .field("name", &self.name)
            .field("expr", &self.expr)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl PhysicalUnaryExpr {
    pub fn create(
        expr: PhysicalExprRef,
        func: UnaryOperator,
        name: String,
        return_type: &DataType,
    ) -> PhysicalExprRef {
        Arc::new(Self {
            expr,
            func,
            name,
            return_type: return_type.clone(),
        })
    }
}

impl PhysicalExpr for PhysicalUnaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> crate::Result<ColumnValue> {
        let value = self.expr.evaluate(input)?;

        let data_type = value.data_type();

        let value_array = value.into_array();

        match self.func {
            UnaryOperator::Abs => unary_arith_op!(abs, data_type, value_array),
            UnaryOperator::Sin => unary_arith_op!(sin, data_type, value_array),
            UnaryOperator::Cos => unary_arith_op!(cos, data_type, value_array),
            UnaryOperator::Tan => unary_arith_op!(cos, data_type, value_array),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::{CsvConfig, CsvTable};
    use crate::error::Result;
    use crate::logical_plan::expression::UnaryOperator;
    use crate::physical_plan::expression::ColumnExpr;
    use crate::physical_plan::PhysicalUnaryExpr;
    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::DataType;

    #[test]
    fn test_abs_expression() -> Result<()> {
        let table = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;
        let abs_expr = PhysicalUnaryExpr::create(
            ColumnExpr::try_create(Some("score".to_string()), None)?,
            UnaryOperator::Abs,
            "abs".to_string(),
            &DataType::Float64,
        );
        let batches = table.scan(Some(vec![3]))?;
        let res = abs_expr.evaluate(&batches[0])?.into_array();
        let score_expected: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(60.0),
            Some(90.1),
            Some(99.99),
            Some(81.1),
            Some(82.2),
            Some(83.3),
            Some(84.4),
            Some(85.5),
        ]));
        assert_eq!(&res, &score_expected);
        Ok(())
    }

    #[test]
    fn test_sin_expression() -> Result<()> {
        let table = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;
        let abs_expr = PhysicalUnaryExpr::create(
            ColumnExpr::try_create(Some("score".to_string()), None)?,
            UnaryOperator::Sin,
            "sin".to_string(),
            &DataType::Float64,
        );
        let batches = table.scan(Some(vec![3]))?;
        let res = abs_expr.evaluate(&batches[0])?.into_array();
        let score_expected: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(-0.3048106211022167),
            Some(0.8447976840197418),
            Some(-0.5149633680424761),
            Some(-0.5492019627147913),
            Some(0.49565689358989423),
            Some(0.9988580516952367),
            Some(0.4104993826174394),
            Some(-0.6264561960895026),
        ]));
        assert_eq!(&res, &score_expected);
        Ok(())
    }
}
