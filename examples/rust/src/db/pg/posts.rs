#[derive(Debug, sqlx::FromRow)]
pub struct Posts {
    pub id: i64,
    pub user_id: i64,
    pub title: String,
    pub body: Option<String>,
}
