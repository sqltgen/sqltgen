#[derive(Debug, sqlx::FromRow)]
pub struct Customer {
    pub id: i64,
    pub name: String,
    pub email: String,
}
