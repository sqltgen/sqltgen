#[derive(Debug, sqlx::FromRow)]
pub struct SciFiBooks {
    pub id: i64,
    pub title: String,
    pub author_name: String,
}
