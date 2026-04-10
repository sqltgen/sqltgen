#[allow(non_camel_case_types)]
#[derive(Debug, sqlx::FromRow)]
pub struct Internal_AuditLog {
    pub id: i64,
    pub user_id: i64,
    pub action: String,
    pub created_at: time::PrimitiveDateTime,
}
