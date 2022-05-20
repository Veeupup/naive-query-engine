/*
 * @Author: Veeupup 
 * @Date: 2022-05-20 19:06:45 
 * @Last Modified by: Veeupup
 * @Last Modified time: 2022-05-20 19:09:11
 */

use crate::Result;
use crate::logical_plan::expression::ScalarValue;
use super::AggregateOperator;

#[derive(Debug, Default)]
pub struct Count {
    cnt: u64,
}

impl AggregateOperator for Count {
    fn update(&mut self, _val: ScalarValue) -> Result<()> {
        self.cnt += 1;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::UInt64(Some(self.cnt)))
    }
}
