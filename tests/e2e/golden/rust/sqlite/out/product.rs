#[derive(Debug, sqlx::FromRow)]
pub struct Product {
    pub id: String,
    pub sku: String,
    pub name: String,
    pub active: i32,
    pub weight_kg: Option<f32>,
    pub rating: Option<f32>,
    pub metadata: Option<String>,
    pub thumbnail: Option<Vec<u8>>,
    pub created_at: String,
    pub stock_count: i32,
}
