/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:45:18
 * @Email: code@tanweime.com
*/

use crate::error::Result;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use super::TableSource;

pub struct CsvTable {
    _filename: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl CsvTable {
    #[allow(unused)]
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
