/*
 * @Author: Veeupup
 * @Date: 2022-05-12 23:36:26
 * @Email: code@tanweime.com
*/

use arrow::datatypes::SchemaRef;
use std::sync::Arc;

use crate::{datasource::EmptyTable, error::Result};
use std::collections::HashMap;

use crate::{
    datasource::{CsvConfig, CsvTable},
    logical_plan::{DataFrame, LogicalPlan, TableScan},
};

pub struct ExecutionContext {
    tables: HashMap<String, DataFrame>,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

impl ExecutionContext {
    pub fn sql(sql: &str) -> DataFrame {
        todo!()
    }

    pub fn csv(&self, filename: &str, projection: Option<Vec<usize>>) -> Result<DataFrame> {
        let source = CsvTable::try_create(filename, CsvConfig::default())?;
        Ok(DataFrame {
            plan: Arc::new(LogicalPlan::TableScan(TableScan { source, projection })),
        })
    }

    pub fn empty(&self, schema: SchemaRef) -> Result<DataFrame> {
        let source = EmptyTable::try_create(schema)?;
        Ok(DataFrame {
            plan: Arc::new(LogicalPlan::TableScan(TableScan {
                source,
                projection: None,
            })),
        })
    }
}
