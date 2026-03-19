/// End-to-end runtime tests for type overrides on SQLite.
///
/// On SQLite:
///  - JSON columns (TEXT) stay as `String` — no native JSON type.
///  - DATETIME columns map to `serde_json::Value` (current codegen behaviour for
///    custom-typed columns). Tests pass datetime strings as JSON strings.
///  - DATE / TIME columns use the `time` crate via sqlx's SQLite driver.
///  - doc_id (UUID stored as TEXT) is a plain `String`.
use e2e_type_overrides_rust_sqlite::db::queries;
use serde_json::{json, Value};
use sqlx::SqlitePool;
use time::macros::{date, time};
use uuid::Uuid;

async fn setup_db() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let ddl = include_str!("../../../../../fixtures/type_overrides/sqlite/schema.sql");
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }
    pool
}

fn ts(s: &str) -> Value {
    Value::String(s.to_string())
}

fn payload_str() -> String {
    serde_json::to_string(&json!({ "type": "click" })).unwrap()
}

fn meta_str() -> String {
    serde_json::to_string(&json!({ "source": "web" })).unwrap()
}

// ─── :one tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_and_get_event() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4().to_string();

    queries::insert_event(
        &pool,
        "login".into(),
        payload_str(),
        Some(meta_str()),
        doc_id.clone(),
        ts("2024-06-01 12:00:00"),
        Some(ts("2024-06-01 14:00:00")),
        Some(date!(2024 - 06 - 01)),
        Some(time!(09:00:00)),
    )
    .await
    .unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.name, "login");
    assert_eq!(ev.doc_id, doc_id);
    // Payload is stored as JSON string; round-trip through serde to verify content
    let payload_back: serde_json::Value = serde_json::from_str(&ev.payload).unwrap();
    assert_eq!(payload_back["type"], "click");
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
        queries::insert_event(&pool, (*name).into(), "{}".into(), None, Uuid::new_v4().to_string(), ts("2024-06-01 12:00:00"), None, None, None).await.unwrap();
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

    for (name, t) in &[("early", "2024-01-01 10:00:00"), ("mid", "2024-06-01 12:00:00"), ("late", "2024-12-01 15:00:00")] {
        queries::insert_event(&pool, (*name).into(), "{}".into(), None, Uuid::new_v4().to_string(), ts(t), None, None, None).await.unwrap();
    }

    let events = queries::get_events_by_date_range(&pool, ts("2024-01-01 00:00:00"), ts("2024-07-01 00:00:00")).await.unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].name, "early");
    assert_eq!(events[1].name, "mid");
}

// ─── :exec tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_update_payload() {
    let pool = setup_db().await;

    queries::insert_event(&pool, "test".into(), r#"{"v":1}"#.into(), None, Uuid::new_v4().to_string(), ts("2024-06-01 12:00:00"), None, None, None)
        .await
        .unwrap();

    queries::update_payload(&pool, r#"{"v":2}"#.into(), None, 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    let payload: serde_json::Value = serde_json::from_str(&ev.payload).unwrap();
    assert_eq!(payload["v"], 2);
    assert!(ev.meta.is_none());
}

#[tokio::test]
async fn test_update_event_date() {
    let pool = setup_db().await;

    queries::insert_event(
        &pool,
        "dated".into(),
        "{}".into(),
        None,
        Uuid::new_v4().to_string(),
        ts("2024-06-01 12:00:00"),
        None,
        Some(date!(2024 - 01 - 01)),
        None,
    )
    .await
    .unwrap();

    queries::update_event_date(&pool, Some(date!(2024 - 12 - 31)), 1).await.unwrap();

    let ev = queries::get_event(&pool, 1).await.unwrap().unwrap();
    assert_eq!(ev.event_date, Some(date!(2024 - 12 - 31)));
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_insert_event_rows() {
    let pool = setup_db().await;

    let n = queries::insert_event_rows(&pool, "rowtest".into(), "{}".into(), None, Uuid::new_v4().to_string(), ts("2024-06-01 12:00:00"), None, None, None)
        .await
        .unwrap();

    assert_eq!(n, 1);
}

// ─── projection tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_find_by_date() {
    let pool = setup_db().await;
    let target = date!(2024 - 06 - 15);

    queries::insert_event(&pool, "dated".into(), "{}".into(), None, Uuid::new_v4().to_string(), ts("2024-06-01 12:00:00"), None, Some(target), None)
        .await
        .unwrap();

    let row = queries::find_by_date(&pool, Some(target)).await.unwrap().unwrap();
    assert_eq!(row.name, "dated");
}

#[tokio::test]
async fn test_find_by_doc_id() {
    let pool = setup_db().await;
    let doc_id = Uuid::new_v4().to_string();

    queries::insert_event(&pool, "uuid-test".into(), "{}".into(), None, doc_id.clone(), ts("2024-06-01 12:00:00"), None, None, None).await.unwrap();

    let row = queries::find_by_doc_id(&pool, doc_id).await.unwrap().unwrap();
    assert_eq!(row.name, "uuid-test");
}

// ─── count tests ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_count_events() {
    let pool = setup_db().await;

    for i in 0..3u32 {
        queries::insert_event(
            &pool,
            format!("ev{i}"),
            "{}".into(),
            None,
            Uuid::new_v4().to_string(),
            ts(&format!("2024-06-{:02} 00:00:00", i + 1)),
            None,
            None,
            None,
        )
        .await
        .unwrap();
    }

    let row = queries::count_events(&pool, ts("2024-01-01 00:00:00")).await.unwrap().unwrap();
    assert_eq!(row.total, 3);
}
