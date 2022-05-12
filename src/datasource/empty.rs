/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:16:58
 * @Email: code@tanweime.com
*/

use super::TableSource;
use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

/// Empty Table with schema but no data
pub struct EmptyTable {
    schema: SchemaRef,
}

impl TableSource for EmptyTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, _projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}
