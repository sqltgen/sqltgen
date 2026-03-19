#[derive(Debug, sqlx::FromRow)]
pub struct Event {
    pub id: i64,
    pub name: String,
    pub payload: serde_json::Value,
    pub meta: Option<serde_json::Value>,
    pub doc_id: String,
    pub created_at: time::PrimitiveDateTime,
    pub scheduled_at: Option<time::PrimitiveDateTime>,
    pub event_date: Option<time::Date>,
    pub event_time: Option<time::Time>,
}
