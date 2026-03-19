use super::_sqltgen::DbPool;

use super::event::Event;

#[derive(Debug, sqlx::FromRow)]
pub struct FindByDateRow {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct FindByDocIdRow {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CountEventsRow {
    pub total: i64,
}

pub async fn get_event(pool: &DbPool, id: i32) -> Result<Option<Event>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        WHERE id = ?
    "##;
    sqlx::query_as::<_, Event>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_events(pool: &DbPool) -> Result<Vec<Event>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        ORDER BY id
    "##;
    sqlx::query_as::<_, Event>(sql)
        .fetch_all(pool)
        .await
}

pub async fn insert_event(pool: &DbPool, name: String, payload: String, meta: Option<String>, doc_id: String, created_at: serde_json::Value, scheduled_at: Option<serde_json::Value>, event_date: Option<time::Date>, event_time: Option<time::Time>) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(payload)
        .bind(meta)
        .bind(doc_id)
        .bind(created_at)
        .bind(scheduled_at)
        .bind(event_date)
        .bind(event_time)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn update_payload(pool: &DbPool, payload: String, meta: Option<String>, id: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        UPDATE event SET payload = ?, meta = ? WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(payload)
        .bind(meta)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn find_by_date(pool: &DbPool, event_date: Option<time::Date>) -> Result<Option<FindByDateRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, name FROM event WHERE event_date = ?
    "##;
    sqlx::query_as::<_, FindByDateRow>(sql)
        .bind(event_date)
        .fetch_optional(pool)
        .await
}

pub async fn find_by_doc_id(pool: &DbPool, doc_id: String) -> Result<Option<FindByDocIdRow>, sqlx::Error> {
    let sql = r##"
        SELECT id, name FROM event WHERE doc_id = ?
    "##;
    sqlx::query_as::<_, FindByDocIdRow>(sql)
        .bind(doc_id)
        .fetch_optional(pool)
        .await
}

pub async fn insert_event_rows(pool: &DbPool, name: String, payload: String, meta: Option<String>, doc_id: String, created_at: serde_json::Value, scheduled_at: Option<serde_json::Value>, event_date: Option<time::Date>, event_time: Option<time::Time>) -> Result<u64, sqlx::Error> {
    let sql = r##"
        INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(payload)
        .bind(meta)
        .bind(doc_id)
        .bind(created_at)
        .bind(scheduled_at)
        .bind(event_date)
        .bind(event_time)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
}

pub async fn get_events_by_date_range(pool: &DbPool, created_at: serde_json::Value, created_at_2: serde_json::Value) -> Result<Vec<Event>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
        FROM event
        WHERE created_at BETWEEN ? AND ?
        ORDER BY created_at
    "##;
    sqlx::query_as::<_, Event>(sql)
        .bind(created_at)
        .bind(created_at_2)
        .fetch_all(pool)
        .await
}

pub async fn count_events(pool: &DbPool, created_at: serde_json::Value) -> Result<Option<CountEventsRow>, sqlx::Error> {
    let sql = r##"
        SELECT COUNT(*) AS total FROM event WHERE created_at > ?
    "##;
    sqlx::query_as::<_, CountEventsRow>(sql)
        .bind(created_at)
        .fetch_optional(pool)
        .await
}

pub async fn update_event_date(pool: &DbPool, event_date: Option<time::Date>, id: i32) -> Result<(), sqlx::Error> {
    let sql = r##"
        UPDATE event SET event_date = ? WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(event_date)
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

    pub async fn get_event(&self, id: i32) -> Result<Option<Event>, sqlx::Error> {
        get_event(self.pool, id).await
    }

    pub async fn list_events(&self) -> Result<Vec<Event>, sqlx::Error> {
        list_events(self.pool).await
    }

    pub async fn insert_event(&self, name: String, payload: String, meta: Option<String>, doc_id: String, created_at: serde_json::Value, scheduled_at: Option<serde_json::Value>, event_date: Option<time::Date>, event_time: Option<time::Time>) -> Result<(), sqlx::Error> {
        insert_event(self.pool, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time).await
    }

    pub async fn update_payload(&self, payload: String, meta: Option<String>, id: i32) -> Result<(), sqlx::Error> {
        update_payload(self.pool, payload, meta, id).await
    }

    pub async fn find_by_date(&self, event_date: Option<time::Date>) -> Result<Option<FindByDateRow>, sqlx::Error> {
        find_by_date(self.pool, event_date).await
    }

    pub async fn find_by_doc_id(&self, doc_id: String) -> Result<Option<FindByDocIdRow>, sqlx::Error> {
        find_by_doc_id(self.pool, doc_id).await
    }

    pub async fn insert_event_rows(&self, name: String, payload: String, meta: Option<String>, doc_id: String, created_at: serde_json::Value, scheduled_at: Option<serde_json::Value>, event_date: Option<time::Date>, event_time: Option<time::Time>) -> Result<u64, sqlx::Error> {
        insert_event_rows(self.pool, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time).await
    }

    pub async fn get_events_by_date_range(&self, created_at: serde_json::Value, created_at_2: serde_json::Value) -> Result<Vec<Event>, sqlx::Error> {
        get_events_by_date_range(self.pool, created_at, created_at_2).await
    }

    pub async fn count_events(&self, created_at: serde_json::Value) -> Result<Option<CountEventsRow>, sqlx::Error> {
        count_events(self.pool, created_at).await
    }

    pub async fn update_event_date(&self, event_date: Option<time::Date>, id: i32) -> Result<(), sqlx::Error> {
        update_event_date(self.pool, event_date, id).await
    }
}
