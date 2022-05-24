use naive_db::print_result;
use naive_db::CsvConfig;
use naive_db::NaiveDB;
use naive_db::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "data/test_data.csv", CsvConfig::default())?;

    // select
    let ret = db.run_sql("select id, name, age + 100 from t1 where id < 6 limit 3")?;
    print_result(&ret)?;

    // Join
    db.create_csv_table("employee", "data/employee.csv", CsvConfig::default())?;
    db.create_csv_table("rank", "data/rank.csv", CsvConfig::default())?;

    let ret = db.run_sql("select * from employee innner join rank on employee.rank = rank.id")?;
    print_result(&ret)?;

    //TODO(ywq support cross_join)
    // let ret = db.run_sql("select * from employee join rank")?;
    // print_result(&ret)?;

    // aggregate
    let ret = db.run_sql("select count(id), sum(age), sum(score) from t1 group by id % 3")?;
    print_result(&ret)?;

    Ok(())
}
