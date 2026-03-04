#[derive(Debug, sqlx::FromRow)]
pub struct Book {
    pub id: i64,
    pub author_id: i64,
    pub title: String,
    pub genre: String,
    pub price: f64,
    pub published_at: Option<time::Date>,
}
