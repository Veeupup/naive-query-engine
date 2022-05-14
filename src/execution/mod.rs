/*
 * @Author: Veeupup
 * @Date: 2022-05-12 23:36:26
 * @Email: code@tanweime.com
*/

use arrow::datatypes::SchemaRef;
use std::sync::Arc;

use crate::{
    datasource::{EmptyTable, TableRef},
    error::{ErrorCode, Result},
};
use std::collections::HashMap;

use crate::{
    datasource::{CsvConfig, CsvTable},
    logical_plan::plan::{LogicalPlan, TableScan},
    logical_plan::DataFrame,
};

#[derive(Default)]
pub struct ExecutionContext {
    #[allow(unused)]
    tables: HashMap<String, DataFrame>,
}

impl ExecutionContext {
    /// create csv table with table name and csv file
    pub fn create_csv_table(&mut self, table: &str, csv_file: &str) -> Result<()> {
        let source = CsvTable::try_create(csv_file, CsvConfig::default())?;
        self.tables.insert(
            table.to_string(),
            DataFrame {
                plan: LogicalPlan::TableScan(TableScan {
                    source,
                    projection: None,
                }),
            },
        );
        Ok(())
    }

    /// get DataFrame
    pub fn get_table_source(&self, table: &str) -> Result<DataFrame> {
        self.tables
            .get(table)
            .cloned()
            .ok_or(ErrorCode::NoSuchTable(format!("No table name: {}", table)))
    }

    /// construct DataFrame just from csv file with no table name
    pub fn csv(&self, filename: &str, projection: Option<Vec<usize>>) -> Result<DataFrame> {
        let source = CsvTable::try_create(filename, CsvConfig::default())?;
        Ok(DataFrame {
            plan: LogicalPlan::TableScan(TableScan { source, projection }),
        })
    }

    pub fn empty(&self, schema: SchemaRef) -> Result<DataFrame> {
        let source = EmptyTable::try_create(schema)?;
        Ok(DataFrame {
            plan: LogicalPlan::TableScan(TableScan {
                source,
                projection: None,
            }),
        })
    }
}
