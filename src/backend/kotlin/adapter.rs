pub(super) use crate::backend::jdbc::{JdbcTarget, JsonBindMode};

/// Resolve the JDBC `JsonBindMode` for the given engine target.
///
/// PostgreSQL requires `setObject(idx, val, Types.OTHER)` for jsonb; MySQL and
/// SQLite work with plain `setString` (or `setObject` for nullable values).
pub(super) fn json_bind_for(target: JdbcTarget) -> JsonBindMode {
    match target {
        JdbcTarget::Postgres => JsonBindMode::TypesOther,
        JdbcTarget::Mysql | JdbcTarget::Sqlite => JsonBindMode::SetString,
    }
}
