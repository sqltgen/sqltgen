#[derive(Debug, sqlx::FromRow)]
pub struct SaleItem {
    pub id: i64,
    pub sale_id: i64,
    pub book_id: i64,
    pub quantity: i32,
    pub unit_price: f64,
}
