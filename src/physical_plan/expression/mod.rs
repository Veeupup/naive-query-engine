/*
 * @Author: Veeupup
 * @Date: 2022-05-13 14:26:45
 * @Email: code@tanweime.com
*/

mod binary;
mod cast;
mod column;
mod literal;
mod unary;

pub use binary::PhysicalBinaryExpr;
pub use cast::PhysicalCastExpr;
pub use column::ColumnExpr;
pub use literal::PhysicalLiteralExpr;
pub use unary::PhysicalUnaryExpr;

use crate::{datatype::ColumnValue, error::Result};
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

pub trait PhysicalExpr: Debug {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnValue>;
}

pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;
