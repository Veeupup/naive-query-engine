/*
 * @Author: Veeupup
 * @Date: 2022-05-14 21:30:10
 * @Email: code@tanweime.com
*/

use crate::logical_plan::expression::ScalarValue;
use std::sync::Arc;

use super::{PhysicalExpr, PhysicalExprRef};
use crate::datatype::ColumnValue;
use crate::Result;
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct PhysicalLiteralExpr {
    pub literal: ScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn create(literal: ScalarValue) -> PhysicalExprRef {
        Arc::new(Self { literal })
    }
}

impl PhysicalExpr for PhysicalLiteralExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue> {
        Ok(ColumnValue::Const(self.literal.clone(), input.num_rows()))
    }
}
