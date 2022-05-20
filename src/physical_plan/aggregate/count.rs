/*
 * @Author: Veeupup 
 * @Date: 2022-05-20 19:06:45 
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 21:13:03
 */

use arrow::datatypes::DataType;

use arrow::record_batch::RecordBatch;

use crate::Result;
use crate::logical_plan::expression::ScalarValue;
use crate::logical_plan::schema::NaiveField;
use crate::physical_plan::ColumnExpr;
use crate::physical_plan::PhysicalExpr;
use super::AggregateOperator;

#[derive(Debug)]
pub struct Count {
    cnt: u64,
    col_expr: ColumnExpr,
}

impl Count {
    pub fn create(col_expr: ColumnExpr) ->Box<dyn AggregateOperator> {
        Box::new(Self {
            cnt: 0,
            col_expr,
        })
    }
}

impl AggregateOperator for Count {
    fn data_field(&self) -> NaiveField{
        NaiveField::new(None, "count(x)", DataType::UInt64, true)
    }

    fn update(&mut self, data: &RecordBatch) -> Result<()> {
        let col = self.col_expr.evaluate(data)?.into_array();
        self.cnt += (col.len() - col.null_count()) as u64;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.cnt)))
    }
}
