use naive_db::print_result;
use naive_db::CsvConfig;
use naive_db::NaiveDB;
use naive_db::Result;

fn main() -> Result<()> {
    let mut db = NaiveDB::default();

    db.create_csv_table("t1", "data/test_data.csv", CsvConfig::default())?;

    // select
    let ret = db.run_sql("select id, name, age + 100 from t1 where id < 9 limit 3 offset 2")?;
    print_result(&ret)?;

    // Join
    db.create_csv_table("employee", "data/employee.csv", CsvConfig::default())?;
    db.create_csv_table("rank", "data/rank.csv", CsvConfig::default())?;
    db.create_csv_table("department", "data/department.csv", CsvConfig::default())?;

    let ret = db.run_sql(
        "
        select id, name, rank_name, department_name
        from employee
        join rank on 
            employee.rank = rank.id  
        join department on
                employee.department_id = department.id
    ",
    )?;
    print_result(&ret)?;

    let ret = db.run_sql("select * from employee join rank")?;
    print_result(&ret)?;

    // aggregate
    let ret = db.run_sql(
        "
        select count(id), sum(age), sum(score), avg(score), max(score), min(score) 
        from t1 group by id % 3",
    )?;
    print_result(&ret)?;

    Ok(())
}
