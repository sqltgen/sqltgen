#[derive(Debug, sqlx::FromRow)]
pub struct Sale {
    pub id: i64,
    pub customer_id: i64,
    pub ordered_at: time::PrimitiveDateTime,
}
