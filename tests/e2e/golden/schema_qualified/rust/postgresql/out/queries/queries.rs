use super::super::sqltgen::DbPool;

use super::super::models::internal_audit_log::Internal_AuditLog;
use super::super::models::users::Users;

pub async fn get_user<'e, E>(executor: E, id: i64) -> Result<Option<Users>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        SELECT * FROM public.users WHERE id = $1
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_optional(executor)
        .await
}

pub async fn list_audit_logs<'e, E>(executor: E) -> Result<Vec<Internal_AuditLog>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        SELECT * FROM internal.audit_log ORDER BY created_at DESC
    "##;
    sqlx::query_as::<_, Internal_AuditLog>(sql)
        .fetch_all(executor)
        .await
}

pub async fn create_audit_log<'e, E>(executor: E, user_id: i64, action: String) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        INSERT INTO internal.audit_log (user_id, action) VALUES ($1, $2)
    "##;
    sqlx::query(sql)
        .bind(user_id)
        .bind(action)
        .execute(executor)
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

    pub async fn get_user(&self, id: i64) -> Result<Option<Users>, sqlx::Error> {
        get_user(self.pool, id).await
    }

    pub async fn list_audit_logs(&self) -> Result<Vec<Internal_AuditLog>, sqlx::Error> {
        list_audit_logs(self.pool).await
    }

    pub async fn create_audit_log(&self, user_id: i64, action: String) -> Result<(), sqlx::Error> {
        create_audit_log(self.pool, user_id, action).await
    }
}
