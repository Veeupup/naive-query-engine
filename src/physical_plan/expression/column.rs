/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:56:36
 * @Email: code@tanweime.com
*/

use std::process::id;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::PhysicalExpression;
use crate::datatype::ColumnValue;
use crate::error::{ErrorCode, Result};

#[derive(Debug, Clone)]
pub struct ColumnExpression {
    name: Option<String>,
    idx: Option<usize>,
}

impl ColumnExpression {
    pub fn try_create(
        name: Option<String>,
        idx: Option<usize>,
    ) -> Result<Arc<dyn PhysicalExpression>> {
        if name.is_none() && idx.is_none() {
            return Err(ErrorCode::LogicalError);
        }
        Ok(Arc::new(Self { name, idx }))
    }
}

impl PhysicalExpression for ColumnExpression {
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
