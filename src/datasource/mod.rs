/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:08:23
 * @Email: code@tanweime.com
*/

mod csv;
mod empty;
mod memory;

use std::fmt::Debug;

use crate::error::Result;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

pub trait TableSource: Debug {
    fn schema(&self) -> SchemaRef;

    // TODO(veeupup): return async stream record batch
    /// for scan
    fn scan(&self, projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>>;
}

pub use csv::CsvConfig;
pub use csv::CsvTable;
pub use empty::EmptyTable;
pub use memory::MemTable;
