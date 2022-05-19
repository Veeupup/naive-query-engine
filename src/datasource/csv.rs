/*
 * @Author: Veeupup
 * @Date: 2022-05-12 16:45:18
 * @Email: code@tanweime.com
*/

use std::env;
use std::fs::File;
use std::iter::Iterator;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::schema::NaiveSchema;

use arrow::csv;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::TableSource;
use crate::datasource::TableRef;

pub struct CsvConfig {
    pub has_header: bool,
    pub delimiter: u8,
    pub max_read_records: Option<usize>,
    pub batch_size: usize,
    pub file_projection: Option<Vec<usize>>,
    pub datetime_format: Option<String>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            max_read_records: Some(3),
            batch_size: 1_000_000,
            file_projection: None,
            datetime_format: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CsvTable {
    schema: NaiveSchema,
    batches: Vec<RecordBatch>,
}

impl CsvTable {
    #[allow(unused, clippy::iter_next_loop)]
    pub fn try_create(filename: &str, csv_config: CsvConfig) -> Result<TableRef> {
        let orig_schema = Self::infer_schema_from_csv(filename, &csv_config)?;
        let schema = NaiveSchema::from_unqualified(&orig_schema);

        let mut file = File::open(env::current_dir()?.join(Path::new(filename)))?;
        let mut reader = csv::Reader::new(
            file,
            Arc::new(orig_schema),
            csv_config.has_header,
            Some(csv_config.delimiter),
            csv_config.batch_size,
            None,
            csv_config.file_projection.clone(),
            csv_config.datetime_format,
        );
        let mut batches = vec![];

        for record in reader.next() {
            batches.push(record?);
        }

        Ok(Arc::new(Self { schema, batches }))
    }

    fn infer_schema_from_csv(filename: &str, csv_config: &CsvConfig) -> Result<Schema> {
        let mut file = File::open(env::current_dir()?.join(Path::new(filename)))?;
        let (schema, _) = arrow::csv::reader::infer_reader_schema(
            &mut file,
            csv_config.delimiter,
            csv_config.max_read_records,
            csv_config.has_header,
        )?;
        Ok(schema)
    }
}

impl TableSource for CsvTable {
    fn schema(&self) -> &NaiveSchema {
        &self.schema
    }

    fn scan(&self, _projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>> {
        Ok(self.batches.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, ArrayRef, Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    #[test]
    fn test_infer_schema() -> Result<()> {
        let table = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;
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

    #[test]
    fn test_read_from_csv() -> Result<()> {
        let table = CsvTable::try_create("data/test_data.csv", CsvConfig::default())?;

        let batches = table.scan(None)?;

        assert_eq!(batches.len(), 1);
        let record_batch = &batches[0];
        assert_eq!(record_batch.columns().len(), 4);

        let id_excepted: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 4, 5, 6, 7, 8, 9]));
        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
            "veeupup", "alex", "lynne", "alice", "bob", "jack", "cock", "primer",
        ]));
        let age_excepted: ArrayRef =
            Arc::new(Int64Array::from(vec![23, 20, 18, 19, 20, 21, 22, 23]));
        let score_excepted: ArrayRef = Arc::new(Float64Array::from(vec![
            60.0, 90.1, 99.99, 81.1, 82.2, 83.3, 84.4, 85.5,
        ]));

        assert_eq!(record_batch.column(0), &id_excepted);
        assert_eq!(record_batch.column(1), &name_excepted);
        assert_eq!(record_batch.column(2), &age_excepted);
        assert_eq!(record_batch.column(3), &score_excepted);

        Ok(())
    }
}
