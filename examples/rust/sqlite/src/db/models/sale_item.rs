#[derive(Debug, sqlx::FromRow)]
pub struct SaleItem {
    pub id: i32,
    pub sale_id: i32,
    pub book_id: i32,
    pub quantity: i32,
    pub unit_price: f64,
}
