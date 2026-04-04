#[derive(Debug, sqlx::FromRow)]
pub struct Product {
    pub id: uuid::Uuid,
    pub sku: String,
    pub name: String,
    pub active: bool,
    pub weight_kg: Option<f32>,
    pub rating: Option<f64>,
    pub tags: Vec<String>,
    pub metadata: Option<serde_json::Value>,
    pub thumbnail: Option<Vec<u8>>,
    pub created_at: time::PrimitiveDateTime,
    pub stock_count: i16,
}
