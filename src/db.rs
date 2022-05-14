/*
 * @Author: Veeupup
 * @Date: 2022-05-14 15:26:40
 * @Email: code@tanweime.com
*/

use arrow::record_batch::RecordBatch;

use crate::catalog::Catalog;
use crate::error::Result;

use crate::optimizer::Optimizer;
use crate::planner::QueryPlanner;
use crate::sql::parser::SQLParser;
use crate::sql::planner::SQLPlanner;

#[derive(Default)]
pub struct NaiveDB {
    catalog: Catalog,
}

impl NaiveDB {
    pub fn run_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // 1. sql -> statement
        let statement = SQLParser::parse(sql)?;
        // 2. statement -> logical plan
        let sql_planner = SQLPlanner::new(&self.catalog);
        let logical_plan = sql_planner.statement_to_plan(statement)?;
        // 3. optimize
        let optimizer = Optimizer::default();
        let logical_plan = optimizer.optimize(logical_plan);
        // 4. logical plan -> physical plan
        let physical_plan = QueryPlanner::create_physical_plan(&logical_plan)?;
        // 5. execute
        physical_plan.execute()
    }

    pub fn create_csv_table(&mut self, table: &str, csv_file: &str) -> Result<()> {
        self.catalog.add_csv_table(table, csv_file)
    }
}
