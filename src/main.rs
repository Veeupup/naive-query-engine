use naive_db::print_result;
use naive_db::NaiveDB;
use naive_db::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "test_data.csv")?;

    let ret = db.run_sql("select id, name, age + 100 from t1 where id < 6 limit 3")?;

    print_result(&ret)?;

    Ok(())
}
