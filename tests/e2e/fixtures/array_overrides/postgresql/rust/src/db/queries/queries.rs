use super::super::sqltgen::DbPool;

use super::super::models::record::Record;

pub async fn insert_record<'e, E>(executor: E, label: String, timestamps: Vec<time::PrimitiveDateTime>, uuids: Vec<uuid::Uuid>) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        INSERT INTO record (label, timestamps, uuids)
        VALUES ($1, $2, $3)
    "##;
    sqlx::query(sql)
        .bind(label)
        .bind(timestamps)
        .bind(uuids)
        .execute(executor)
        .await
        .map(|_| ())
}

pub async fn get_record<'e, E>(executor: E, id: i64) -> Result<Option<Record>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        SELECT id, label, timestamps, uuids
        FROM record
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Record>(sql)
        .bind(id)
        .fetch_optional(executor)
        .await
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn insert_record(&self, label: String, timestamps: Vec<time::PrimitiveDateTime>, uuids: Vec<uuid::Uuid>) -> Result<(), sqlx::Error> {
        insert_record(self.pool, label, timestamps, uuids).await
    }

    pub async fn get_record(&self, id: i64) -> Result<Option<Record>, sqlx::Error> {
        get_record(self.pool, id).await
    }
}
