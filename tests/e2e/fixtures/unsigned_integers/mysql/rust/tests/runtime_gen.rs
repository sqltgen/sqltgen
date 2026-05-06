use e2e_unsigned_integers_rust_mysql::db::queries::queries;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;

fn root_url() -> String {
    std::env::var("MYSQL_ROOT_URL").unwrap_or_else(|_| "mysql://root:sqltgen@localhost:13306".into())
}

fn user_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "mysql://sqltgen:sqltgen@localhost:13306".into())
}

async fn setup_db() -> (MySqlPool, String) {
    let db_name = format!("testgen_{}", uuid::Uuid::new_v4().simple());
    let admin = MySqlPool::connect(&format!("{}/sqltgen_e2e", root_url())).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE `{db_name}`")).execute(&admin).await.unwrap();
    sqlx::query(&format!("GRANT ALL ON `{db_name}`.* TO 'sqltgen'@'%'")).execute(&admin).await.unwrap();
    admin.close().await;
    let pool = MySqlPoolOptions::new().connect(&format!("{}/{db_name}", user_url())).await.unwrap();
    let ddl = include_str!("../../schema.sql");
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }
    (pool, db_name)
}

async fn teardown(pool: MySqlPool, db_name: String) {
    pool.close().await;
    let admin = MySqlPool::connect(&format!("{}/sqltgen_e2e", root_url())).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS `{db_name}`")).execute(&admin).await.unwrap();
    admin.close().await;
}

#[tokio::test]
async fn unsigned_integers_round_trip_through_full_range() {
    let (pool, db_name) = setup_db().await;
    // Row 1: zero. Row 2: small. Row 3: each column at its maximum unsigned value.
    queries::insert_unsigned_row(&pool, 0, 0, 0, 0, 0).await.unwrap();
    queries::insert_unsigned_row(&pool, 1, 1, 1, 1, 1).await.unwrap();
    queries::insert_unsigned_row(&pool, u8::MAX, u16::MAX, 16_777_215, u32::MAX, u64::MAX).await.unwrap();

    let rows = queries::get_unsigned_rows(&pool).await.unwrap();
    assert_eq!(rows.len(), 3);

    assert_eq!((rows[0].u8_val, rows[0].u16_val, rows[0].u24_val, rows[0].u32_val, rows[0].u64_val), (0, 0, 0, 0, 0));
    assert_eq!((rows[1].u8_val, rows[1].u16_val, rows[1].u24_val, rows[1].u32_val, rows[1].u64_val), (1, 1, 1, 1, 1));
    assert_eq!(rows[2].u8_val, u8::MAX);
    assert_eq!(rows[2].u16_val, u16::MAX);
    assert_eq!(rows[2].u24_val, 16_777_215_u32);
    assert_eq!(rows[2].u32_val, u32::MAX);
    // The critical correctness gate: 2^64-1 must round-trip without truncation.
    assert_eq!(rows[2].u64_val, u64::MAX);
    // The id column itself is BIGINT UNSIGNED.
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[2].id, 3);

    teardown(pool, db_name).await;
}
