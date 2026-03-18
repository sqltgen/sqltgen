/// Engine-specific contract for C++ code generation.
///
/// Each engine (PostgreSQL, SQLite, MySQL) uses a different C/C++ client
/// library with different connection types, include paths, and parameter
/// binding styles. This contract captures those differences so the shared
/// generation logic in `core.rs` never branches on target.

/// How SQL placeholders should be rewritten for the target engine's client.
#[derive(Clone, Copy)]
pub(super) enum CppParamStyle {
    /// PostgreSQL libpqxx: uses `$1`, `$2`, … (kept as-is from the IR).
    Dollar,
    /// SQLite sqlite3: uses `?1`, `?2`, … (already in IR for SQLite frontend,
    /// but Postgres-originated SQL needs rewriting).
    QuestionNumbered,
    /// MySQL libmysqlclient: uses `?` (anonymous positional).
    QuestionAnon,
}

/// Resolved engine-specific contract consumed by `core.rs` emitters.
pub(super) struct CppEngineContract {
    /// Primary `#include` for the database client (e.g. `<pqxx/pqxx>`).
    pub(super) db_include: &'static str,
    /// The C++ type used for a database connection parameter
    /// (e.g. `pqxx::connection&`, `sqlite3*`, `MYSQL*`).
    pub(super) conn_type: &'static str,
    /// Placeholder style used by this engine's client library.
    pub(super) param_style: CppParamStyle,
}

pub(super) fn resolve_contract(target: &super::CppTarget) -> CppEngineContract {
    match target {
        super::CppTarget::Postgres => CppEngineContract {
            db_include: "<pqxx/pqxx>",
            conn_type: "pqxx::connection&",
            param_style: CppParamStyle::Dollar,
        },
        super::CppTarget::Sqlite => CppEngineContract {
            db_include: "<sqlite3.h>",
            conn_type: "sqlite3*",
            param_style: CppParamStyle::QuestionNumbered,
        },
        super::CppTarget::Mysql => CppEngineContract {
            db_include: "<mysql/mysql.h>",
            conn_type: "MYSQL*",
            param_style: CppParamStyle::QuestionAnon,
        },
    }
}
