/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:45:18
 * @Email: code@tanweime.com
*/

use crate::error::Result;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use super::datasource::TableSource;

pub struct CsvTable {
    filename: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl CsvTable {
    pub fn try_create(_filename: &str) -> Result<Self> {
        todo!()
    }
}

impl TableSource for CsvTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, _projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>> {
        Ok(self.batches.clone())
    }
}
