use super::*;

#[test]
fn test_generate_queries_module_uses_helper_pool_alias() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);

    for files in [
        pg().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
        sqlite().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
        mysql().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap(),
    ] {
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("use super::_sqltgen::DbPool;"));
        assert!(!src.contains("use sqlx::{PgPool as DbPool};"));
        assert!(!src.contains("use sqlx::{SqlitePool as DbPool};"));
        assert!(!src.contains("use sqlx::{MySqlPool as DbPool};"));
    }
}

#[test]
fn test_generate_rust_helper_module_per_target() {
    let schema = Schema::default();
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);

    let pg_files = pg().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap();
    assert!(get_file(&pg_files, "_sqltgen.rs").contains("pub type DbPool = sqlx::PgPool;"));

    let sq_files = sqlite().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap();
    assert!(get_file(&sq_files, "_sqltgen.rs").contains("pub type DbPool = sqlx::SqlitePool;"));

    let my_files = mysql().generate(&schema, std::slice::from_ref(&query), &cfg()).unwrap();
    assert!(get_file(&my_files, "_sqltgen.rs").contains("pub type DbPool = sqlx::MySqlPool;"));
}
