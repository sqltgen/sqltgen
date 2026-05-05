use super::super::sqltgen::DbPool;

use super::super::models::task::Task;
use super::super::models::priority::Priority;
use super::super::models::status::Status;

#[derive(Debug, sqlx::FromRow)]
pub struct GetTaskRow {
    pub id: i64,
    pub title: String,
    pub priority: Priority,
    pub status: Status,
    pub description: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListTasksByPriorityRow {
    pub id: i64,
    pub title: String,
    pub priority: Priority,
    pub status: Status,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListTasksByStatusRow {
    pub id: i64,
    pub title: String,
    pub priority: Priority,
    pub status: Status,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListTasksByPriorityOrAllRow {
    pub id: i64,
    pub title: String,
    pub priority: Priority,
    pub status: Status,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CountByStatusRow {
    pub status: Status,
    pub task_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetTaskTagsRow {
    pub id: i64,
    pub title: String,
    pub tags: Vec<Priority>,
}

pub async fn create_task(pool: &DbPool, title: String, priority: Priority, status: Status, description: Option<String>) -> Result<Option<Task>, sqlx::Error> {
    let sql = r##"
        INSERT INTO task (title, priority, status, description)
        VALUES ($1, $2, $3, $4)
        RETURNING *
    "##;
    sqlx::query_as::<_, Task>(sql)
        .bind(title)
        .bind(priority)
        .bind(status)
        .bind(description)
        .fetch_optional(pool)
        .await
}

pub async fn get_task(pool: &DbPool, id: i64) -> Result<Option<GetTaskRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, priority, status, description
        FROM task
        WHERE id = $1
    "##;
    sqlx::query_as::<_, GetTaskRow>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_tasks_by_priority(pool: &DbPool, priority: Priority) -> Result<Vec<ListTasksByPriorityRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, priority, status
        FROM task
        WHERE priority = $1
        ORDER BY id
    "##;
    sqlx::query_as::<_, ListTasksByPriorityRow>(sql)
        .bind(priority)
        .fetch_all(pool)
        .await
}

pub async fn list_tasks_by_status(pool: &DbPool, status: Status) -> Result<Vec<ListTasksByStatusRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, priority, status
        FROM task
        WHERE status = $1
        ORDER BY id
    "##;
    sqlx::query_as::<_, ListTasksByStatusRow>(sql)
        .bind(status)
        .fetch_all(pool)
        .await
}

pub async fn update_task_status(pool: &DbPool, status: Status, id: i64) -> Result<Option<Task>, sqlx::Error> {
    let sql = r##"
        UPDATE task SET status = $1 WHERE id = $2
        RETURNING *
    "##;
    sqlx::query_as::<_, Task>(sql)
        .bind(status)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_tasks_by_priority_or_all(pool: &DbPool, priority: Option<Priority>) -> Result<Vec<ListTasksByPriorityOrAllRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, priority, status
        FROM task
        WHERE ($1::priority IS NULL OR priority = $1::priority)
        ORDER BY id
    "##;
    sqlx::query_as::<_, ListTasksByPriorityOrAllRow>(sql)
        .bind(priority)
        .fetch_all(pool)
        .await
}

pub async fn count_by_status(pool: &DbPool) -> Result<Vec<CountByStatusRow>, sqlx::Error> {
    let sql = r##"
        SELECT status, COUNT(*) AS task_count
        FROM task
        GROUP BY status
        ORDER BY status
    "##;
    sqlx::query_as::<_, CountByStatusRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn create_task_with_tags(pool: &DbPool, title: String, priority: Priority, status: Status, description: Option<String>, tags: Vec<Priority>) -> Result<Option<Task>, sqlx::Error> {
    let sql = r##"
        INSERT INTO task (title, priority, status, description, tags)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
    "##;
    sqlx::query_as::<_, Task>(sql)
        .bind(title)
        .bind(priority)
        .bind(status)
        .bind(description)
        .bind(tags)
        .fetch_optional(pool)
        .await
}

pub async fn get_task_tags(pool: &DbPool, id: i64) -> Result<Option<GetTaskTagsRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, title, tags
        FROM task
        WHERE id = $1
    "##;
    sqlx::query_as::<_, GetTaskTagsRow>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn update_task_tags(pool: &DbPool, tags: Vec<Priority>, id: i64) -> Result<Option<Task>, sqlx::Error> {
    let sql = r##"
        UPDATE task SET tags = $1 WHERE id = $2
        RETURNING *
    "##;
    sqlx::query_as::<_, Task>(sql)
        .bind(tags)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn delete_task(pool: &DbPool, id: i64) -> Result<(), sqlx::Error> {
    let sql = r##"
        DELETE FROM task WHERE id = $1
    "##;
    sqlx::query(sql)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn create_task(&self, title: String, priority: Priority, status: Status, description: Option<String>) -> Result<Option<Task>, sqlx::Error> {
        create_task(self.pool, title, priority, status, description).await
    }

    pub async fn get_task(&self, id: i64) -> Result<Option<GetTaskRow>, sqlx::Error> {
        get_task(self.pool, id).await
    }

    pub async fn list_tasks_by_priority(&self, priority: Priority) -> Result<Vec<ListTasksByPriorityRow>, sqlx::Error> {
        list_tasks_by_priority(self.pool, priority).await
    }

    pub async fn list_tasks_by_status(&self, status: Status) -> Result<Vec<ListTasksByStatusRow>, sqlx::Error> {
        list_tasks_by_status(self.pool, status).await
    }

    pub async fn update_task_status(&self, status: Status, id: i64) -> Result<Option<Task>, sqlx::Error> {
        update_task_status(self.pool, status, id).await
    }

    pub async fn list_tasks_by_priority_or_all(&self, priority: Option<Priority>) -> Result<Vec<ListTasksByPriorityOrAllRow>, sqlx::Error> {
        list_tasks_by_priority_or_all(self.pool, priority).await
    }

    pub async fn count_by_status(&self) -> Result<Vec<CountByStatusRow>, sqlx::Error> {
        count_by_status(self.pool).await
    }

    pub async fn create_task_with_tags(&self, title: String, priority: Priority, status: Status, description: Option<String>, tags: Vec<Priority>) -> Result<Option<Task>, sqlx::Error> {
        create_task_with_tags(self.pool, title, priority, status, description, tags).await
    }

    pub async fn get_task_tags(&self, id: i64) -> Result<Option<GetTaskTagsRow>, sqlx::Error> {
        get_task_tags(self.pool, id).await
    }

    pub async fn update_task_tags(&self, tags: Vec<Priority>, id: i64) -> Result<Option<Task>, sqlx::Error> {
        update_task_tags(self.pool, tags, id).await
    }

    pub async fn delete_task(&self, id: i64) -> Result<(), sqlx::Error> {
        delete_task(self.pool, id).await
    }
}
