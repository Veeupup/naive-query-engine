use naive_db::print_result;
use naive_db::NaiveDB;
use naive_db::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "data/test_data.csv")?;

    let ret = db.run_sql("select id, name, age + 100 from t1 where id < 6 limit 3")?;

    print_result(&ret)?;

    // Join
    db.create_csv_table("employee", "data/employee.csv")?;
    db.create_csv_table("rank", "data/rank.csv")?;

    let ret = db.run_sql(
        "select id, name, rank_name from employee innner join rank on employee.rank = rank.id",
    )?;

    print_result(&ret);
    Ok(())
}
