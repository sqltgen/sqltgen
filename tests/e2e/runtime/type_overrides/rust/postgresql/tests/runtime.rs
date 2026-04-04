/// End-to-end runtime tests for type overrides: serde_json + time crate on PostgreSQL.
///
/// Each test creates an isolated PostgreSQL schema so tests can run in parallel.
/// Requires the docker-compose postgres service on port 15432.
use e2e_type_overrides_rust_postgresql::db::queries::queries;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use time::{
    macros::{date, datetime, time},
    OffsetDateTime, PrimitiveDateTime,
};
use uuid::Uuid;

// ─── Setup helpers ────────────────────────────────────────────────────────────

async fn setup_db() -> PgPool {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://sqltgen:sqltgen@localhost:15432/sqltgen_e2e".into());

    let bootstrap = PgPool::connect(&url).await.unwrap();
    let schema = format!("test_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\"")).execute(&bootstrap).await.unwrap();
    bootstrap.close().await;

    let pool = PgPoolOptions::new()
        .after_connect({
            let schema = schema.clone();
            move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    sqlx::query(&format!("SET search_path TO \"{schema}\"")).execute(&mut *conn).await?;
                    Ok(())
                })
            }
        })
        .connect(&url)
        .await
        .unwrap();

    let ddl = include_str!("../../../../../fixtures/type_overrides/postgresql/schema.sql");
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }

    pool
}

fn sample_payload() -> serde_json::Value {
    json!({ "type": "click", "x": 10, "y": 20 })
}

fn sample_meta() -> serde_json::Value {
    json!({ "source": "web", "version": 1 })
}

fn sample_created_at() -> PrimitiveDateTime {
    datetime!(2024-06-01 12:00:00)
}

fn sample_scheduled_at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
}

// ─── :one tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_and_get_event() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4();

    queries::insert_event(
        &pool,
        "login".into(),
        sample_payload(),
        Some(sample_meta()),
        doc_id,
        sample_created_at(),
        Some(sample_scheduled_at()),
        Some(date!(2024 - 06 - 01)),
        Some(time!(09:00:00)),
    )
    .await
    .unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.name, "login");
    assert_eq!(ev.payload, sample_payload());
    assert_eq!(ev.meta, Some(sample_meta()));
    assert_eq!(ev.doc_id, doc_id);
    assert_eq!(ev.created_at, sample_created_at());
    assert_eq!(ev.scheduled_at, Some(sample_scheduled_at()));
    assert_eq!(ev.event_date, Some(date!(2024 - 06 - 01)));
    assert_eq!(ev.event_time, Some(time!(09:00:00)));
}

#[tokio::test]
async fn test_get_event_not_found() {
    let pool = setup_db().await;
    let result = queries::get_event(&pool, 999).await.unwrap();
    assert!(result.is_none());
}

// ─── :many tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_list_events() {
    let pool = setup_db().await;

    for name in &["alpha", "beta", "gamma"] {
        queries::insert_event(&pool, (*name).into(), json!({ "n": name }), None, Uuid::new_v4(), sample_created_at(), None, None, None).await.unwrap();
    }

    let events = queries::list_events(&pool).await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].name, "alpha");
    assert_eq!(events[1].name, "beta");
    assert_eq!(events[2].name, "gamma");
}

#[tokio::test]
async fn test_get_events_by_date_range() {
    let pool = setup_db().await;

    let t1 = datetime!(2024-01-01 10:00:00);
    let t2 = datetime!(2024-06-01 12:00:00);
    let t3 = datetime!(2024-12-01 15:00:00);

    for (name, ts) in &[("early", t1), ("mid", t2), ("late", t3)] {
        queries::insert_event(&pool, (*name).into(), json!({}), None, Uuid::new_v4(), *ts, None, None, None).await.unwrap();
    }

    let range_start = datetime!(2024-01-01 00:00:00);
    let range_end = datetime!(2024-07-01 00:00:00);
    let events = queries::get_events_by_date_range(&pool, range_start, range_end).await.unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].name, "early");
    assert_eq!(events[1].name, "mid");
}

// ─── :exec tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_update_payload() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4();

    queries::insert_event(&pool, "test".into(), json!({ "v": 1 }), Some(json!({"source": "web"})), doc_id, sample_created_at(), None, None, None).await.unwrap();

    let updated = json!({ "v": 2, "changed": true });
    queries::update_payload(&pool, updated.clone(), None, 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.payload, updated);
    assert_eq!(ev.meta, None);
}

#[tokio::test]
async fn test_update_event_date() {
    let pool = setup_db().await;

    queries::insert_event(&pool, "dated".into(), json!({}), None, Uuid::new_v4(), sample_created_at(), None, Some(date!(2024 - 01 - 01)), None).await.unwrap();

    queries::update_event_date(&pool, Some(date!(2024 - 12 - 31)), 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.event_date, Some(date!(2024 - 12 - 31)));
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_event_rows() {
    let pool = setup_db().await;

    let rows_affected =
        queries::insert_event_rows(&pool, "rowtest".into(), json!({ "k": "v" }), None, Uuid::new_v4(), sample_created_at(), None, None, None).await.unwrap();

    assert_eq!(rows_affected, 1);
}

// ─── :one projection tests ────────────────────────────────────────────────────

#[tokio::test]
async fn test_find_by_date() {
    let pool = setup_db().await;
    let target_date = date!(2024 - 06 - 15);

    queries::insert_event(&pool, "dated".into(), json!({}), None, Uuid::new_v4(), sample_created_at(), None, Some(target_date), None).await.unwrap();

    let row = queries::find_by_date(&pool, Some(target_date)).await.unwrap().unwrap();
    assert_eq!(row.name, "dated");
}

#[tokio::test]
async fn test_find_by_uuid() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4();

    queries::insert_event(&pool, "uuid-test".into(), json!({}), None, doc_id, sample_created_at(), None, None, None).await.unwrap();

    let row = queries::find_by_uuid(&pool, doc_id).await.unwrap().unwrap();
    assert_eq!(row.name, "uuid-test");
}

// ─── count tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_events() {
    let pool = setup_db().await;
    let cutoff = datetime!(2024-01-01 00:00:00);

    for i in 0..3u32 {
        let ts = datetime!(2024-06-01 00:00:00) + time::Duration::days(i64::from(i));
        queries::insert_event(&pool, format!("ev{i}"), json!({}), None, Uuid::new_v4(), ts, None, None, None).await.unwrap();
    }

    let row = queries::count_events(&pool, cutoff).await.unwrap().unwrap();
    assert_eq!(row.total, 3);
}
