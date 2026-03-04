#[derive(Debug, sqlx::FromRow)]
pub struct Posts {
    pub id: i32,
    pub user_id: i32,
    pub title: String,
    pub body: Option<String>,
}
