use how_query_engine_work::print_result;
use how_query_engine_work::NaiveDB;
use how_query_engine_work::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "test_data.csv")?;

    let ret = db.run_sql("select id, name from t1")?;

    print_result(&ret)?;

    Ok(())
}
