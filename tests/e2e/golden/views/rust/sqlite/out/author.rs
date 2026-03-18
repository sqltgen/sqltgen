#[derive(Debug, sqlx::FromRow)]
pub struct Author {
    pub id: i32,
    pub name: String,
}
