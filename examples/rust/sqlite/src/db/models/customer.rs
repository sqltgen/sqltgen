#[derive(Debug, sqlx::FromRow)]
pub struct Customer {
    pub id: i32,
    pub name: String,
    pub email: String,
}
