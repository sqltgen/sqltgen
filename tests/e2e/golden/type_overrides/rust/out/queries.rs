use sqlx::PgPool;

use super::event::Event;

#[derive(Debug, sqlx::FromRow)]
pub struct FindByDateRow {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct FindByUuidRow {
    pub id: i64,
    pub name: String,
}

pub async fn get_event(pool: &PgPool, id: i64) -> Result<Option<Event>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Event>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_events(pool: &PgPool) -> Result<Vec<Event>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        ORDER BY id
    "##;
    sqlx::query_as::<_, Event>(sql)
        .fetch_all(pool)
        .await
}

pub async fn insert_event(pool: &PgPool, name: String, payload: serde_json::Value, meta: Option<serde_json::Value>, doc_id: uuid::Uuid, created_at: time::PrimitiveDateTime, scheduled_at: Option<time::OffsetDateTime>, event_date: Option<time::Date>, event_time: Option<time::Time>) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(payload)
        .bind(meta)
        .bind(doc_id)
        .bind(created_at)
        .bind(scheduled_at)
        .bind(event_date)
        .bind(event_time)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn update_payload(pool: &PgPool, payload: serde_json::Value, meta: Option<serde_json::Value>, id: i64) -> Result<(), sqlx::Error> {
    let sql = r##"
        UPDATE event SET payload = $1, meta = $2 WHERE id = $3
    "##;
    sqlx::query(sql)
        .bind(payload)
        .bind(meta)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn find_by_date(pool: &PgPool, event_date: Option<time::Date>) -> Result<Option<FindByDateRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, name FROM event WHERE event_date = $1
    "##;
    sqlx::query_as::<_, FindByDateRow>(sql)
        .bind(event_date)
        .fetch_optional(pool)
        .await
}

pub async fn find_by_uuid(pool: &PgPool, doc_id: uuid::Uuid) -> Result<Option<FindByUuidRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, name FROM event WHERE doc_id = $1
    "##;
    sqlx::query_as::<_, FindByUuidRow>(sql)
        .bind(doc_id)
        .fetch_optional(pool)
        .await
}
