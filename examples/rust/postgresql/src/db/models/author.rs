#[derive(Debug, sqlx::FromRow)]
pub struct Author {
    pub id: i64,
    pub name: String,
    pub bio: Option<String>,
    pub birth_year: Option<i32>,
}
