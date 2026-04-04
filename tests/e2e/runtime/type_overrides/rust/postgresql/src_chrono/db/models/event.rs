#[derive(Debug, sqlx::FromRow)]
pub struct Event {
    pub id: i64,
    pub name: String,
    pub payload: serde_json::Value,
    pub meta: Option<serde_json::Value>,
    pub doc_id: uuid::Uuid,
    pub created_at: chrono::NaiveDateTime,
    pub scheduled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_date: Option<chrono::NaiveDate>,
    pub event_time: Option<chrono::NaiveTime>,
}
