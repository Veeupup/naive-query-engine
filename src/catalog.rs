/*
 * @Author: Veeupup
 * @Date: 2022-05-14 16:00:32
 * @Email: code@tanweime.com
*/

use std::collections::HashMap;

use crate::datasource::{EmptyTable, MemTable};
use crate::error::ErrorCode;
use crate::logical_plan::plan::{LogicalPlan, TableScan};
use crate::logical_plan::schema::NaiveSchema;
use crate::logical_plan::DataFrame;
use crate::{
    datasource::{CsvConfig, CsvTable, TableRef},
    error::Result,
};
use arrow::record_batch::RecordBatch;

#[derive(Default)]
pub struct Catalog {
    tables: HashMap<String, TableRef>,
}

impl Catalog {
    /// add csv table
    pub fn add_csv_table(
        &mut self,
        table: &str,
        csv_file: &str,
        csv_conf: CsvConfig,
    ) -> Result<()> {
        let source = CsvTable::try_create(csv_file, csv_conf)?;
        self.tables.insert(table.to_string(), source);
        Ok(())
    }

    /// add memory table
    pub fn add_memory_table(
        &mut self,
        table: &str,
        schema: NaiveSchema,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let source = MemTable::try_create(schema, batches)?;
        self.tables.insert(table.to_string(), source);
        Ok(())
    }

    /// add empty table
    pub fn add_empty_table(&mut self, table: &str, schema: NaiveSchema) -> Result<()> {
        let source = EmptyTable::try_create(schema)?;
        self.tables.insert(table.to_string(), source);
        Ok(())
    }

    /// get table
    pub fn get_table(&self, table: &str) -> Result<TableRef> {
        self.tables
            .get(table)
            .cloned()
            .ok_or_else(|| ErrorCode::NoSuchTable(format!("No table name: {}", table)))
    }

    /// get dataframe by table name
    pub fn get_table_df(&self, table: &str) -> Result<DataFrame> {
        let source = self
            .tables
            .get(table)
            .cloned()
            .ok_or_else(|| ErrorCode::NoSuchTable(format!("No table name: {}", table)))?;
        let plan = LogicalPlan::TableScan(TableScan {
            source,
            projection: None,
        });
        Ok(DataFrame { plan })
    }
}
