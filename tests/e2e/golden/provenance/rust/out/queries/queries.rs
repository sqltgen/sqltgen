use super::super::sqltgen::DbPool;

use super::super::models::users::Users;

pub async fn get_user_via_derived<'e, E>(executor: E, id: i64) -> Result<Option<Users>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        SELECT * FROM (SELECT * FROM users) AS sub
        WHERE sub.id = $1
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_optional(executor)
        .await
}

pub async fn get_users_via_cte<'e, E>(executor: E, id: i64) -> Result<Vec<Users>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        WITH recent AS (SELECT * FROM users WHERE id > $1)
        SELECT * FROM recent
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_all(executor)
        .await
}

pub async fn get_users_via_chained_ctes<'e, E>(executor: E, id: i64) -> Result<Vec<Users>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let sql = r##"
        WITH a AS (SELECT * FROM users),
             b AS (SELECT * FROM a)
        SELECT * FROM b
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_all(executor)
        .await
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn get_user_via_derived(&self, id: i64) -> Result<Option<Users>, sqlx::Error> {
        get_user_via_derived(self.pool, id).await
    }

    pub async fn get_users_via_cte(&self, id: i64) -> Result<Vec<Users>, sqlx::Error> {
        get_users_via_cte(self.pool, id).await
    }

    pub async fn get_users_via_chained_ctes(&self, id: i64) -> Result<Vec<Users>, sqlx::Error> {
        get_users_via_chained_ctes(self.pool, id).await
    }
}
