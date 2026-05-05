#[derive(Debug, sqlx::FromRow)]
pub struct Record {
    pub id: i64,
    pub label: String,
    pub timestamps: Vec<time::PrimitiveDateTime>,
    pub uuids: Vec<uuid::Uuid>,
}
