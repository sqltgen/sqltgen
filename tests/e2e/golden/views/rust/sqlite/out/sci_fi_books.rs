#[derive(Debug, sqlx::FromRow)]
pub struct SciFiBooks {
    pub id: i32,
    pub title: String,
    pub author_name: String,
}
