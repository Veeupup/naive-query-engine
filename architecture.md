# Architecture

## datatype

use `arrow` as datatype system.

## datasource

we can return scan result by using async streaming trait after

## 

```rust
pub struct NaiveDB {
    catalog: HashMap<String, TableRef>
};

impl NaiveDB {
    pub run_sql(sql: &str) -> Result<RecordBatch> {
        let statement = Parser::parse_sql();
        let logical_plan = SqlPlanner::sql_to_plan(statement, catalog)?;
        let optimizer = Optimizer::new();
        let plan = optimizer.optimize(plan);
        let physical_plan = QueryPlanner::create_physical_plan(plan);
        plan.execute()
    }
}

fn main() {
    
    DB::run_sql("select a, b from t where a > 1")
}


```
