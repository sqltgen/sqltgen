use sqlx::{PgPool as DbPool};

use super::users::Users;

pub async fn get_user_via_derived(pool: &DbPool, id: i64) -> Result<Option<Users>, sqlx::Error> {
    let sql = r##"
        SELECT * FROM (SELECT * FROM users) AS sub
        WHERE sub.id = $1
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_users_via_cte(pool: &DbPool, id: i64) -> Result<Vec<Users>, sqlx::Error> {
    let sql = r##"
        WITH recent AS (SELECT * FROM users WHERE id > $1)
        SELECT * FROM recent
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_all(pool)
        .await
}

pub async fn get_users_via_chained_ctes(pool: &DbPool, id: i64) -> Result<Vec<Users>, sqlx::Error> {
    let sql = r##"
        WITH a AS (SELECT * FROM users),
             b AS (SELECT * FROM a)
        SELECT * FROM b
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_all(pool)
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
