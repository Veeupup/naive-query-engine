/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:26:45
 * @Email: code@tanweime.com
*/

mod binary;
mod column;
mod literal;
mod scalar;

pub use binary::PhysicalBinaryExpr;
pub use column::ColumnExpr;
pub use literal::PhysicalLiteralExpr;
pub use scalar::PhysicalScalarExpr;

use crate::{datatype::ColumnValue, error::Result};
use arrow::record_batch::RecordBatch;
use std::fmt::Debug;
use std::sync::Arc;

pub trait PhysicalExpr: Debug {
    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue>;
}

pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;
