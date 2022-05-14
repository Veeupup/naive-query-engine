/*
 * @Author: Veeupup
 * @Date: 2022-05-14 16:00:32
 * @Email: code@tanweime.com
*/

use std::collections::HashMap;

use crate::error::ErrorCode;
use crate::{
    datasource::{CsvConfig, CsvTable, TableRef},
    error::Result,
};

#[derive(Default)]
pub struct Catalog {
    tables: HashMap<String, TableRef>,
}

impl Catalog {
    pub fn add_csv_table(&mut self, table: &str, csv_file: &str) -> Result<()> {
        let source = CsvTable::try_create(csv_file, CsvConfig::default())?;
        self.tables.insert(table.to_string(), source);
        Ok(())
    }

    pub fn get_table(&self, table: &str) -> Result<TableRef> {
        self.tables
            .get(table)
            .cloned()
            .ok_or(ErrorCode::NoSuchTable(format!("No table name: {}", table)))
    }
}
