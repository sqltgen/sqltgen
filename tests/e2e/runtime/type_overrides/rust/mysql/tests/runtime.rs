/// End-to-end runtime tests for type overrides on MySQL.
///
/// JSON columns use serde_json::Value (native JSON type).
/// Datetime columns use time::PrimitiveDateTime.
/// UUID is stored as CHAR(36) — represented as String.
///
/// Each test creates an isolated MySQL database for isolation.
/// Requires the docker-compose MySQL service on port 13306.
use e2e_type_overrides_rust_mysql::db::queries;
use serde_json::json;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::MySqlPool;
use time::macros::{date, datetime, time};
use uuid::Uuid;

fn root_url() -> String {
    std::env::var("MYSQL_ROOT_URL").unwrap_or_else(|_| "mysql://root:sqltgen@localhost:13306".into())
}

fn test_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| "mysql://sqltgen:sqltgen@localhost:13306".into())
}

async fn setup_db() -> (MySqlPool, String) {
    let db_name = format!("test_to_{}", Uuid::new_v4().simple());

    let admin = MySqlPool::connect(&format!("{}/sqltgen_e2e", root_url())).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE `{db_name}`")).execute(&admin).await.unwrap();
    sqlx::query(&format!("GRANT ALL ON `{db_name}`.* TO 'sqltgen'@'%'")).execute(&admin).await.unwrap();
    admin.close().await;

    let pool = MySqlPoolOptions::new().connect(&format!("{}/{db_name}", test_url())).await.unwrap();

    let ddl = include_str!("../../../../../fixtures/type_overrides/mysql/schema.sql");
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }

    (pool, db_name)
}

async fn teardown(pool: MySqlPool, db_name: &str) {
    pool.close().await;
    let admin = MySqlPool::connect(&format!("{}/sqltgen_e2e", root_url())).await.unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS `{db_name}`")).execute(&admin).await.unwrap();
    admin.close().await;
}

fn sample_created_at() -> time::PrimitiveDateTime {
    datetime!(2024-06-01 12:00:00)
}

// ─── :one tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_and_get_event() {
    let (pool, db_name) = setup_db().await;
    let doc_id = Uuid::new_v4().to_string();
    let payload = json!({ "type": "click", "x": 10 });
    let meta = json!({ "source": "mysql" });

    queries::insert_event(
        &pool,
        "login".into(),
        payload.clone(),
        Some(meta.clone()),
        doc_id.clone(),
        sample_created_at(),
        None,
        Some(date!(2024 - 06 - 01)),
        Some(time!(09:00:00)),
    )
    .await
    .unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.name, "login");
    assert_eq!(ev.payload, payload);
    assert_eq!(ev.meta, Some(meta));
    assert_eq!(ev.doc_id, doc_id);
    assert_eq!(ev.created_at, sample_created_at());
    assert_eq!(ev.event_date, Some(date!(2024 - 06 - 01)));
    assert_eq!(ev.event_time, Some(time!(09:00:00)));

    teardown(pool, &db_name).await;
}

#[tokio::test]
async fn test_get_event_not_found() {
    let (pool, db_name) = setup_db().await;
    let result = queries::get_event(&pool, 999).await.unwrap();
    assert!(result.is_none());
    teardown(pool, &db_name).await;
}

// ─── :many tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_events() {
    let (pool, db_name) = setup_db().await;

    for name in &["alpha", "beta", "gamma"] {
        queries::insert_event(&pool, (*name).into(), json!({ "n": name }), None, Uuid::new_v4().to_string(), sample_created_at(), None, None, None)
            .await
            .unwrap();
    }

    let events = queries::list_events(&pool).await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].name, "alpha");

    teardown(pool, &db_name).await;
}

#[tokio::test]
async fn test_get_events_by_date_range() {
    let (pool, db_name) = setup_db().await;

    let t1 = datetime!(2024-01-01 10:00:00);
    let t2 = datetime!(2024-06-01 12:00:00);
    let t3 = datetime!(2024-12-01 15:00:00);

    for (name, ts) in &[("early", t1), ("mid", t2), ("late", t3)] {
        queries::insert_event(&pool, (*name).into(), json!({}), None, Uuid::new_v4().to_string(), *ts, None, None, None).await.unwrap();
    }

    let events = queries::get_events_by_date_range(&pool, datetime!(2024-01-01 00:00:00), datetime!(2024-07-01 00:00:00)).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].name, "early");
    assert_eq!(events[1].name, "mid");

    teardown(pool, &db_name).await;
}

// ─── :exec tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_update_payload() {
    let (pool, db_name) = setup_db().await;

    queries::insert_event(&pool, "test".into(), json!({ "v": 1 }), None, Uuid::new_v4().to_string(), sample_created_at(), None, None, None).await.unwrap();

    let updated = json!({ "v": 2 });
    queries::update_payload(&pool, updated.clone(), None, 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.payload, updated);

    teardown(pool, &db_name).await;
}

#[tokio::test]
async fn test_update_event_date() {
    let (pool, db_name) = setup_db().await;

    queries::insert_event(&pool, "dated".into(), json!({}), None, Uuid::new_v4().to_string(), sample_created_at(), None, Some(date!(2024 - 01 - 01)), None)
        .await
        .unwrap();

    queries::update_event_date(&pool, Some(date!(2024 - 12 - 31)), 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.event_date, Some(date!(2024 - 12 - 31)));

    teardown(pool, &db_name).await;
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_event_rows() {
    let (pool, db_name) = setup_db().await;

    let n =
        queries::insert_event_rows(&pool, "rowtest".into(), json!({}), None, Uuid::new_v4().to_string(), sample_created_at(), None, None, None).await.unwrap();

    assert_eq!(n, 1);
    teardown(pool, &db_name).await;
}

// ─── projection tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_find_by_date() {
    let (pool, db_name) = setup_db().await;
    let target = date!(2024 - 06 - 15);

    queries::insert_event(&pool, "dated".into(), json!({}), None, Uuid::new_v4().to_string(), sample_created_at(), None, Some(target), None).await.unwrap();

    let row = queries::find_by_date(&pool, Some(target)).await.unwrap().unwrap();
    assert_eq!(row.name, "dated");

    teardown(pool, &db_name).await;
}

#[tokio::test]
async fn test_find_by_doc_id() {
    let (pool, db_name) = setup_db().await;
    let doc_id = Uuid::new_v4().to_string();

    queries::insert_event(&pool, "doc-test".into(), json!({}), None, doc_id.clone(), sample_created_at(), None, None, None).await.unwrap();

    let row = queries::find_by_doc_id(&pool, doc_id).await.unwrap().unwrap();
    assert_eq!(row.name, "doc-test");

    teardown(pool, &db_name).await;
}

// ─── count tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_events() {
    let (pool, db_name) = setup_db().await;
    let cutoff = datetime!(2024-01-01 00:00:00);

    for i in 0..3u32 {
        let ts = datetime!(2024-06-01 00:00:00) + time::Duration::days(i64::from(i));
        queries::insert_event(&pool, format!("ev{i}"), json!({}), None, Uuid::new_v4().to_string(), ts, None, None, None).await.unwrap();
    }

    let row = queries::count_events(&pool, cutoff).await.unwrap().unwrap();
    assert_eq!(row.total, 3);

    teardown(pool, &db_name).await;
}
