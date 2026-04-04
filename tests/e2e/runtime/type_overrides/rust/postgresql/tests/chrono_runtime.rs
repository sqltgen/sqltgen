/// End-to-end runtime tests for type overrides: serde_json + chrono on PostgreSQL.
///
/// Uses the same fixture schema but a separate sqltgen config (`sqltgen-chrono.json`)
/// that maps timestamp/date/time types to chrono types via explicit TypeRef overrides.
/// The generated code lives in `src_chrono/` to avoid conflicts with the main `src/`.
///
/// Each test creates an isolated PostgreSQL schema so tests can run in parallel.
/// Requires the docker-compose postgres service on port 15432.
#[path = "../src_chrono/lib.rs"]
mod chrono_db_crate;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use chrono_db_crate::db::queries::queries;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

// ─── Setup helpers ────────────────────────────────────────────────────────────

async fn setup_db() -> PgPool {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://sqltgen:sqltgen@localhost:15432/sqltgen_e2e".into());

    let bootstrap = PgPool::connect(&url).await.unwrap();
    let schema = format!("test_chrono_{}", Uuid::new_v4().simple());
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

fn sample_created_at() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2024, 6, 1).unwrap().and_hms_opt(12, 0, 0).unwrap()
}

fn sample_scheduled_at() -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).unwrap()
}

// ─── :one tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chrono_insert_and_get_event() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4();
    let payload = json!({ "type": "chrono-test" });
    let meta = json!({ "lib": "chrono" });
    let event_date = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap();
    let event_time = NaiveTime::from_hms_opt(9, 0, 0).unwrap();

    queries::insert_event(
        &pool,
        "chrono-event".into(),
        payload.clone(),
        Some(meta.clone()),
        doc_id,
        sample_created_at(),
        Some(sample_scheduled_at()),
        Some(event_date),
        Some(event_time),
    )
    .await
    .unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.name, "chrono-event");
    assert_eq!(ev.payload, payload);
    assert_eq!(ev.meta, Some(meta));
    assert_eq!(ev.doc_id, doc_id);
    assert_eq!(ev.created_at, sample_created_at());
    assert_eq!(ev.event_date, Some(event_date));
    assert_eq!(ev.event_time, Some(event_time));
}

#[tokio::test]
async fn test_chrono_get_event_not_found() {
    let pool = setup_db().await;
    let result = queries::get_event(&pool, 999).await.unwrap();
    assert!(result.is_none());
}

// ─── :many tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chrono_list_events() {
    let pool = setup_db().await;

    for name in &["a", "b", "c"] {
        queries::insert_event(&pool, (*name).into(), json!({}), None, Uuid::new_v4(), sample_created_at(), None, None, None).await.unwrap();
    }

    let events = queries::list_events(&pool).await.unwrap();
    assert_eq!(events.len(), 3);
}

#[tokio::test]
async fn test_chrono_get_events_by_date_range() {
    let pool = setup_db().await;

    let ts = |y, m, d| NaiveDate::from_ymd_opt(y, m, d).unwrap().and_hms_opt(0, 0, 0).unwrap();

    for (name, t) in &[("early", ts(2024, 1, 1)), ("mid", ts(2024, 6, 1)), ("late", ts(2024, 12, 1))] {
        queries::insert_event(&pool, (*name).into(), json!({}), None, Uuid::new_v4(), *t, None, None, None).await.unwrap();
    }

    let events = queries::get_events_by_date_range(&pool, ts(2024, 1, 1), ts(2024, 7, 1)).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].name, "early");
    assert_eq!(events[1].name, "mid");
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chrono_insert_event_rows() {
    let pool = setup_db().await;

    let n = queries::insert_event_rows(&pool, "rowtest".into(), json!({}), None, Uuid::new_v4(), sample_created_at(), None, None, None).await.unwrap();

    assert_eq!(n, 1);
}

// ─── :exec tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_chrono_update_payload() {
    let pool = setup_db().await;

    queries::insert_event(&pool, "test".into(), json!({ "v": 1 }), None, Uuid::new_v4(), sample_created_at(), None, None, None).await.unwrap();

    let updated = json!({ "v": 2 });
    queries::update_payload(&pool, updated.clone(), None, 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.payload, updated);
}

#[tokio::test]
async fn test_chrono_update_event_date() {
    let pool = setup_db().await;

    queries::insert_event(
        &pool,
        "dated".into(),
        json!({}),
        None,
        Uuid::new_v4(),
        sample_created_at(),
        None,
        Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
        None,
    )
    .await
    .unwrap();

    let new_date = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
    queries::update_event_date(&pool, Some(new_date), 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.event_date, Some(new_date));
}

// ─── projection + count tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_chrono_find_by_date() {
    let pool = setup_db().await;
    let target = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();

    queries::insert_event(&pool, "dated".into(), json!({}), None, Uuid::new_v4(), sample_created_at(), None, Some(target), None).await.unwrap();

    let row = queries::find_by_date(&pool, Some(target)).await.unwrap().unwrap();
    assert_eq!(row.name, "dated");
}

#[tokio::test]
async fn test_chrono_find_by_uuid() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4();

    queries::insert_event(&pool, "uuid-test".into(), json!({}), None, doc_id, sample_created_at(), None, None, None).await.unwrap();

    let row = queries::find_by_uuid(&pool, doc_id).await.unwrap().unwrap();
    assert_eq!(row.name, "uuid-test");
}

#[tokio::test]
async fn test_chrono_count_events() {
    let pool = setup_db().await;
    let cutoff = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();

    for i in 0..3u32 {
        let ts = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap().and_hms_opt(0, 0, 0).unwrap() + chrono::Duration::days(i64::from(i));
        queries::insert_event(&pool, format!("ev{i}"), json!({}), None, Uuid::new_v4(), ts, None, None, None).await.unwrap();
    }

    let row = queries::count_events(&pool, cutoff).await.unwrap().unwrap();
    assert_eq!(row.total, 3);
}
