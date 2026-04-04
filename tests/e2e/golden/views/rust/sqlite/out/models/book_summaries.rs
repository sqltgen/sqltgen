#[derive(Debug, sqlx::FromRow)]
pub struct BookSummaries {
    pub id: i32,
    pub title: String,
    pub genre: String,
    pub author_name: String,
}
