#[derive(Debug, sqlx::FromRow)]
pub struct Book {
    pub id: i32,
    pub author_id: i32,
    pub title: String,
    pub genre: String,
}
