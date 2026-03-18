#[derive(Debug, sqlx::FromRow)]
pub struct Author {
    pub id: i64,
    pub name: String,
}
