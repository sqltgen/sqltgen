#[derive(Debug, sqlx::FromRow)]
pub struct Event {
    pub id: i32,
    pub name: String,
    pub payload: String,
    pub meta: Option<String>,
    pub doc_id: String,
    pub created_at: time::PrimitiveDateTime,
    pub scheduled_at: Option<time::PrimitiveDateTime>,
    pub event_date: Option<time::Date>,
    pub event_time: Option<time::Time>,
}
