/*
 * @Author: ywqzzy
 * @Date: 2022-05-20
*/

use arrow::{datatypes::DataType, record_batch::RecordBatch};
use core::fmt;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use super::{PhysicalExpr, PhysicalExprRef};
use crate::datatype::ColumnValue;

pub struct PhysicalCastExpr {
    #[allow(unused)]
    expr: PhysicalExprRef,
    data_type: DataType,
}

impl Debug for PhysicalCastExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("CastExpr")
            .field("name", &"CAST")
            .field("return_type", &self.data_type)
            .finish()
    }
}

impl PhysicalCastExpr {
    pub fn create(expr: PhysicalExprRef, data_type: &DataType) -> PhysicalExprRef {
        Arc::new(Self {
            expr,
            data_type: data_type.clone(),
        })
    }
}

impl PhysicalExpr for PhysicalCastExpr {
    fn evaluate(&self, _input: &RecordBatch) -> crate::Result<ColumnValue> {
        // let value = self.expr.evaluate(input)?;

        // let from_data_type = value.data_type();
        // let value_array = value.into_array();
        // let field_array_builder = arrow::array::make_builder(&self.data_type, input.num_rows());

        match self.data_type {
            DataType::Null => todo!(),
            DataType::Boolean => todo!(),
            DataType::Int8 => todo!(),
            DataType::Int16 => todo!(),
            DataType::Int32 => todo!(),
            DataType::Int64 => todo!(),
            DataType::UInt8 => todo!(),
            DataType::UInt16 => todo!(),
            DataType::UInt32 => todo!(),
            DataType::UInt64 => todo!(),
            DataType::Float16 => todo!(),
            DataType::Float32 => todo!(),
            DataType::Float64 => todo!(),
            DataType::Timestamp(_, _) => todo!(),
            DataType::Date32 => todo!(),
            DataType::Date64 => todo!(),
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            DataType::Binary => todo!(),
            DataType::FixedSizeBinary(_) => todo!(),
            DataType::LargeBinary => todo!(),
            DataType::Utf8 => todo!(),
            DataType::LargeUtf8 => todo!(),
            DataType::List(_) => todo!(),
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(_) => todo!(),
            DataType::Union(_, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal(_, _) => todo!(),
            DataType::Map(_, _) => todo!(),
        }
    }
}
