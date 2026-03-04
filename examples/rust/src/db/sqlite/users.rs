#[derive(Debug, sqlx::FromRow)]
pub struct Users {
    pub id: i32,
    pub name: String,
    pub email: String,
    pub bio: Option<String>,
}
