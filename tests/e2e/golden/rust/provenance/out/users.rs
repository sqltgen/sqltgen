#[derive(Debug, sqlx::FromRow)]
pub struct Users {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub bio: Option<String>,
}
