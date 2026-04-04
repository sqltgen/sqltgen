#[derive(Debug, sqlx::FromRow)]
pub struct Sale {
    pub id: i32,
    pub customer_id: i32,
    pub ordered_at: time::PrimitiveDateTime,
}
