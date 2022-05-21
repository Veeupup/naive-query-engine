use naive_db::print_result;
use naive_db::CsvConfig;
use naive_db::NaiveDB;
use naive_db::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "data/test_data.csv", CsvConfig::default())?;

    let ret = db.run_sql("select * from t1")?;

    print_result(&ret)?;

    // Join
    db.create_csv_table("employee", "data/employee.csv", CsvConfig::default())?;
    db.create_csv_table("rank", "data/rank.csv", CsvConfig::default())?;

    let ret = db.run_sql("select * from employee innner join rank on employee.rank = rank.id")?;

    print_result(&ret)?;
    Ok(())
}
