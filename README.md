# Naive Query Engine (Toy for Learning) 😄

This is a Query Engine which support `SQL` interface. And it is only a Toy for learn query engine only. You can check [TODO](https://github.com/Veeupup/naive-query-engine#todo) to check the progress now.

Simple enough to learn (Although it is simple...but with so much work to finish.. TAT 😭) and Now it only has a basic architecture and most operators and planners have not implemented (will be done in the future).

This is inspired(and most ideas come) by [how-query-engines-work](https://github.com/andygrove/how-query-engines-work) and it is just for learning purpose. And many ideas inspired by [arrow-datafusion](https://github.com/apache/arrow-datafusion).

Use [arrow](https://github.com/apache/arrow-rs) to express in-memory columnar format and use [sqlparser](https://github.com/sqlparser-rs/sqlparser-rs) as SQL parser.

## how to use

for now, we can use `NaiveDB` like below, we can use csv as table storage.

```rust
use naive_db::print_result;
use naive_db::NaiveDB;
use naive_db::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "data/test_data.csv")?;

    let ret = db.run_sql("select id, name, age + 100 from t1 where id < 6 limit 3")?;

    print_result(&ret)?;

    Ok(())
}
```

output will be:

```
+----+---------+-----------+
| id | name    | age + 100 |
+----+---------+-----------+
| 1  | veeupup | 123       |
| 2  | alex    | 120       |
| 4  | lynne   | 118       |
+----+---------+-----------+
```

## architecture

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
    - [x] projection
    - [x] filter
    - [x] aggregate
    - [x] limit
    - [ ] join and more...
- [x] physical plan & expressions
    - [x] physical scan
    - [x] physical projection
    - [x] physical filter
    - [x] physical limit
    - [ ] physical expression
        - [x] column expr
        - [x] binary operation expr(add/sub/mul/div/and/or...)
        - [x] literal expr
        - [ ] so many work to do... TAT
- [ ] query planner
    - [x] scan
    - [x] limit
    - [ ] aggregate
    - [ ] join
    - [ ] ...
- [ ] query optimization
    - [ ] more rules needed
- [ ] sql support
    - [x] parser
    - [ ] SQL planner: statement -> logical plan
        - [x] scan
        - [x] projection
        - [x] selection
        - [x] limit
        - [ ] join and more...
