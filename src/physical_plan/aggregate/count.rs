/*
 * @Author: Veeupup
 * @Date: 2022-05-20 19:06:45
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:13:03
 */

use arrow::datatypes::DataType;

use arrow::record_batch::RecordBatch;

use super::AggregateOperator;
use crate::error::ErrorCode;
use crate::logical_plan::expression::ScalarValue;
use crate::logical_plan::schema::NaiveField;
use crate::physical_plan::aggregate::NaiveSchema;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;
use crate::Result;

#[derive(Debug, Clone)]
pub struct Count {
    cnt: u64,
    col_expr: ColumnExpr,
}

impl Count {
    pub fn create(col_expr: ColumnExpr) -> Box<dyn AggregateOperator> {
        Box::new(Self { cnt: 0, col_expr })
    }
}

impl AggregateOperator for Count {
    fn data_field(&self, schema: &NaiveSchema) -> Result<NaiveField> {
        // find by name
        if let Some(name) = &self.col_expr.name {
            let field = schema.field_with_unqualified_name(name)?;
            return Ok(NaiveField::new(
                None,
                format!("count({})", field.name()).as_str(),
                DataType::UInt64,
                false,
            ));
        }

        if let Some(idx) = &self.col_expr.idx {
            let field = schema.field(*idx);
            return Ok(NaiveField::new(
                None,
                format!("count({})", field.name()).as_str(),
                DataType::UInt64,
                false,
            ));
        }

        Err(ErrorCode::LogicalError(
            "ColumnExpr must has name or idx".to_string(),
        ))
    }

    fn update_batch(&mut self, data: &RecordBatch) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        self.cnt += (col.len() - col.null_count()) as u64;
        Ok(())
    }

    fn update(&mut self, data: &RecordBatch, idx: usize) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        if !col.is_null(idx) {
            self.cnt += 1;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.cnt)))
    }

    fn clear_state(&mut self) {
        self.cnt = 0;
    }
}
