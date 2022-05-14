/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:56:36
 * @Email: code@tanweime.com
*/

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::PhysicalExpr;
use crate::datatype::ColumnValue;
use crate::error::{ErrorCode, Result};
use crate::physical_plan::PhysicalExprRef;

#[derive(Debug, Clone)]
pub struct ColumnExpr {
    name: Option<String>,
    idx: Option<usize>,
}

impl ColumnExpr {
    pub fn try_create(name: Option<String>, idx: Option<usize>) -> Result<PhysicalExprRef> {
        if name.is_none() && idx.is_none() {
            return Err(ErrorCode::LogicalError);
        }
        Ok(Arc::new(Self { name, idx }))
    }
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue> {
        // prefer idx first
        if let Some(idx) = self.idx {
            let column = input.column(idx).clone();
            return Ok(ColumnValue::Array(column));
        }
        // then name
        if let Some(name) = &self.name {
            for (idx, field) in input.schema().fields().iter().enumerate() {
                if field.name() == name {
                    let column = input.column(idx).clone();
                    return Ok(ColumnValue::Array(column));
                }
            }
        }
        Err(ErrorCode::LogicalError)
    }
}
