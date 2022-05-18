/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:16:58
 * @Email: code@tanweime.com
*/

use super::TableSource;
use crate::datasource::TableRef;
use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Empty Table with schema but no data
#[derive(Debug, Clone)]
pub struct EmptyTable {
    schema: NaiveSchema,
}

impl EmptyTable {
    #[allow(unused)]
    pub fn try_create(schema: NaiveSchema) -> Result<TableRef> {
        Ok(Arc::new(Self { schema }))
    }
}

impl TableSource for EmptyTable {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn scan(&self, _projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    

    #[test]
    fn test_empty_table() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let schema = NaiveSchema::from_qualified("t1", &schema);

        let table = EmptyTable::try_create(schema)?;
        let batches = table.scan(None)?;

        assert!(batches.is_empty());

        Ok(())
    }
}
