#[derive(Debug, sqlx::FromRow)]
pub struct Event {
    pub id: i32,
    pub name: String,
    pub payload: String,
    pub meta: Option<String>,
    pub doc_id: String,
    pub created_at: serde_json::Value,
    pub scheduled_at: Option<serde_json::Value>,
    pub event_date: Option<time::Date>,
    pub event_time: Option<time::Time>,
}
