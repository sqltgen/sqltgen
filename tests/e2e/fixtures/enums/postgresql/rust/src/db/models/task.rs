use super::priority::Priority;
use super::status::Status;

#[derive(Debug, sqlx::FromRow)]
pub struct Task {
    pub id: i64,
    pub title: String,
    pub priority: Priority,
    pub status: Status,
    pub description: Option<String>,
    pub tags: Vec<Priority>,
}
