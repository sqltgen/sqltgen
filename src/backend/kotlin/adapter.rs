pub(super) use crate::backend::jdbc::{JdbcTarget, JsonBindMode, JvmCoreContract};

/// Resolve the Kotlin adapter contract for the given engine target.
pub(super) fn resolve_kotlin_contract(target: JdbcTarget) -> JvmCoreContract {
    let json_bind = match target {
        JdbcTarget::Postgres => JsonBindMode::TypesOther,
        JdbcTarget::Mysql | JdbcTarget::Sqlite => JsonBindMode::SetString,
    };
    JvmCoreContract { statement_end: "", fallback_type: "Any", size_access: ".size", json_bind }
}
