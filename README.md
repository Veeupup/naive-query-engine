# How Query Engine Work (Toy for Learning)

This is a Query Engine which support `SQL` interface. And it is only a Toy for learn query engine only. 

Simple enough to learn and Now it only has a basic architecture and most operators and planners have not implemented (will be done in the future).

This is inspired(and most ideas come) by [how-query-engines-work](https://github.com/andygrove/how-query-engines-work) and it is just for learning purpose. And many ideas inspired by [arrow-datafusion](https://github.com/apache/arrow-datafusion).

Use [arrow](https://github.com/apache/arrow-rs) to express in-memory columnar format and use [sqlparser](https://github.com/sqlparser-rs/sqlparser-rs) as SQL parser.

## how to use

for now, we can use `NaiveDB` like below, we can use csv as table storage.

```rust
use how_query_engine_work::print_result;
use how_query_engine_work::NaiveDB;
use how_query_engine_work::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "test_data.csv")?;

    let ret = db.run_sql("select id, name from t1")?;

    print_result(&ret);

    Ok(())
}
```

output will be:

```
+----+---------+
| id | name    |
+----+---------+
| 1  | veeupup |
| 2  | alex    |
| 4  | lynne   |
+----+---------+
```

## architure

The NaiveDB is just simple and has clear progress just like:

```rust
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
}
```


## TODO

- [x] type system
- [x] datasource
    - [x] mem source
    - [x] csv as datasource
    - [x] empty datasource
- [x] logical plan & expressions
- [ ] build logical plans
- [ ] physical plan & expressions
    - [ ] many task need to implement
- [ ] query planner
    - [ ] many task need to implement
- [ ] query optimization
    - [ ] more rules needed
- [ ] query execution
- [ ] sql support
    - [ ] SQL planner support more sqls
