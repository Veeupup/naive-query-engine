/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:45:18
 * @Email: code@tanweime.com
*/

use std:: env;
use std::sync::Arc;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::Result;

use arrow::csv;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};

use super::TableSource;

pub struct CsvTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl CsvTable {
    #[allow(unused)]
    pub fn try_create(filename: &str) -> Result<Self> {
        let path = env::current_dir()?;
        let path = path.join(Path::new(filename));
        println!("{:?}", path);
        let mut file = File::open(path)?;
        let schema = Self::infer_schema(&mut file)?;
        println!("{:?}", schema);
        // let reader = csv::Reader::new(
        //     file,
        // );
        Ok(Self {
            schema,
            batches: vec![],
        })
    }

    fn infer_schema(file: &mut File) -> Result<SchemaRef> {
        // let mut schemas = vec![];
        let (schema, records_read) = arrow::csv::reader::infer_reader_schema(
            file,
            b',',
            Some(3),
            true,
        )?;
        Ok(Arc::new(schema))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_infer_schema() -> Result<()> {
        let table = CsvTable::try_create("test_schema.txt")?;
        let schema = table.schema();

        let excepted = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
            Field::new("score", DataType::Float64, false),
        ]));

        assert_eq!(schema.fields().len(), excepted.fields().len());
        
        let iter = schema.fields().iter().zip(excepted.fields().iter());
        for (field, excepted) in iter {
            assert_eq!(field.name(), excepted.name());
            assert_eq!(field.data_type(), excepted.data_type());
            assert_eq!(field.is_nullable(), excepted.is_nullable());
        }

        Ok(())
    }

}
