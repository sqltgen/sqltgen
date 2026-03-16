use sqlx::PgPool;

use super::users::Users;

pub async fn get_user_via_derived(pool: &PgPool, id: i64) -> Result<Option<Users>, sqlx::Error> {
    let sql = r##"
        SELECT * FROM (SELECT * FROM users) AS sub
        WHERE sub.id = $1
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_users_via_cte(pool: &PgPool, id: i64) -> Result<Vec<Users>, sqlx::Error> {
    let sql = r##"
        WITH recent AS (SELECT * FROM users WHERE id > $1)
        SELECT * FROM recent
    "##;
    sqlx::query_as::<_, Users>(sql)
        .bind(id)
        .fetch_all(pool)
        .await
}

pub async fn get_users_via_chained_ctes(pool: &PgPool, id: i64) -> Result<Vec<Users>, sqlx::Error> {
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
