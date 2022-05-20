/*
 * @Author: Veeupup 
 * @Date: 2022-05-20 19:09:44 
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 19:14:21
 */

use crate::Result;
use crate::logical_plan::expression::ScalarValue;
use super::AggregateOperator;

#[derive(Debug, Default)]
pub struct Sum {
    // TODO(veeupup): should use generic type for Int64, UInt Float64
    sum: i64,
}

impl AggregateOperator for Sum {
    fn update(&mut self, val: ScalarValue) -> Result<()> {
        match val {
            ScalarValue::Int64(Some(val)) => self.sum += val,
            _ => unimplemented!()
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.sum)))   
    }
}
