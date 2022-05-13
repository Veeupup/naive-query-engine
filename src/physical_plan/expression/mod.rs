/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:26:45
 * @Email: code@tanweime.com
*/

mod column;

pub use column::ColumnExpression;

use crate::{datatype::ColumnValue, error::Result};
use arrow::record_batch::RecordBatch;
use std::fmt::Debug;

pub trait PhysicalExpression: Debug {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue>;
}
