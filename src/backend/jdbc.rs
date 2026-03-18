use std::fmt::Write;

use crate::backend::common::{infer_row_type_name, infer_table, jdbc_bind_sequence, jdbc_setter, pg_array_type_name, sql_const_name};
use crate::backend::naming::{to_camel_case, to_pascal_case};
use crate::backend::sql_rewrite::{rewrite_to_anon_params, split_at_in_clause};
use crate::config::{Engine, ListParamStrategy};
use crate::ir::{NativeListBind, Parameter, Query, QueryCmd, Schema, SqlType};

/// Database engine target shared by all JDBC backends (Java, Kotlin).
///
/// Replaces the per-backend `JavaTarget`/`KotlinTarget` enums which were
/// identical. Used for strategy dispatch in list-param code generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JdbcTarget {
    Postgres,
    Mysql,
    Sqlite,
}

impl From<Engine> for JdbcTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => JdbcTarget::Postgres,
            Engine::Mysql => JdbcTarget::Mysql,
            Engine::Sqlite => JdbcTarget::Sqlite,
        }
    }
}

/// Escape a SQL string for embedding in a generated string literal.
///
/// Replaces newlines with spaces and double-quotes with escaped double-quotes.
pub fn escape_sql(sql: &str) -> String {
    sql.replace('\n', " ").replace('"', "\\\"")
}

/// Resolve the row type name for a query result, with a language-specific fallback.
///
/// Returns the inferred type name (table name or `{Query}Row`) or `fallback`
/// when the query has no result columns.
pub fn result_row_type(query: &Query, schema: &Schema, fallback: &str) -> String {
    infer_row_type_name(query, schema).unwrap_or_else(|| fallback.to_string())
}

/// Like [`result_row_type`], but qualifies inline row types as `{class_name}.XxxRow`
/// because they are inner types of the static queries class/object.
///
/// `class_name` is the name of the enclosing class (e.g. `"Queries"`, `"UsersQueries"`).
pub fn ds_result_row_type(query: &Query, schema: &Schema, fallback: &str, class_name: &str) -> String {
    if let Some(table_name) = infer_table(query, schema) {
        return to_pascal_case(table_name);
    }
    if !query.result_columns.is_empty() {
        return format!("{class_name}.{}Row", to_pascal_case(&query.name));
    }
    fallback.to_string()
}

/// Emit JDBC `ps.setXxx(idx, value)` calls for each parameter in bind order.
///
/// For the list parameter slot, emits a bind using `list_bind_expr` (e.g. `"arr"`
/// for PostgreSQL arrays, `"json"` for JSON strings). `se` is the statement-end
/// string: `";"` for Java, `""` for Kotlin. `to_array_call` is the method to
/// convert a `List` to a plain array for `createArrayOf`: `"toArray()"` for Java,
/// `"toTypedArray()"` for Kotlin.
/// Emit `ps.setXxx(...)` calls for all query parameters.
///
/// `scalar_value_expr` is called for every non-list, non-array parameter and returns
/// the expression to bind — normally just the camel-case param name, but backends can
/// substitute a `write_expr` here (e.g. `objectMapper.writeValueAsString(payload)`).
pub fn emit_jdbc_binds<F>(src: &mut String, query: &Query, list_bind_expr: &str, se: &str, to_array_call: &str, scalar_value_expr: F) -> anyhow::Result<()>
where
    F: Fn(&Parameter) -> String,
{
    let list_setter = if list_bind_expr == "arr" { "setArray" } else { "setString" };
    for (jdbc_idx, p) in jdbc_bind_sequence(query) {
        if p.is_list {
            writeln!(src, "            ps.{list_setter}({jdbc_idx}, {list_bind_expr}){se}")?;
        } else if let SqlType::Array(inner) = &p.sql_type {
            let name = to_camel_case(&p.name);
            let type_name = pg_array_type_name(inner);
            writeln!(src, "            ps.setArray({jdbc_idx}, conn.createArrayOf(\"{type_name}\", {name}.{to_array_call})){se}")?;
        } else if matches!(p.sql_type, SqlType::Json | SqlType::Jsonb) {
            writeln!(src, "            ps.setObject({jdbc_idx}, {}, java.sql.Types.OTHER){se}", scalar_value_expr(p))?;
        } else if p.nullable {
            writeln!(src, "            ps.setObject({jdbc_idx}, {}){se}", scalar_value_expr(p))?;
        } else {
            writeln!(src, "            ps.{}({jdbc_idx}, {}){se}", jdbc_setter(&p.sql_type), scalar_value_expr(p))?;
        }
    }
    Ok(())
}

/// Compute the return type string for a JDBC query method.
///
/// `one_fmt` and `many_fmt` are format functions for wrapping the row type.
/// `exec_type` and `exec_rows_type` are the literal type strings for
/// exec and execrows commands.
pub fn jdbc_return_type(
    query: &Query,
    schema: &Schema,
    fallback: &str,
    one_fmt: fn(&str) -> String,
    many_fmt: fn(&str) -> String,
    exec_type: &str,
    exec_rows_type: &str,
) -> String {
    let row = result_row_type(query, schema, fallback);
    match query.cmd {
        QueryCmd::One => one_fmt(&row),
        QueryCmd::Many => many_fmt(&row),
        QueryCmd::Exec => exec_type.to_string(),
        QueryCmd::ExecRows => exec_rows_type.to_string(),
    }
}

/// The resolved list-param action for JDBC backends.
///
/// Used by [`resolve_list_strategy`] to communicate which code path to take,
/// along with any pre-computed rewritten SQL.
pub enum ListAction {
    /// PostgreSQL native: `= ANY(?)` with a JDBC array. Contains rewritten SQL.
    PgNative(String),
    /// Dynamic: runtime `IN (?,?,…,?)` expansion.
    Dynamic,
    /// JSON-based native (SQLite `json_each` / MySQL `JSON_TABLE`). Contains rewritten SQL.
    JsonNative(String),
}

/// Resolve the list-param action for a given strategy setting and parameter.
///
/// Uses the pre-computed `native_list_sql` and `native_list_bind` from the IR
/// (set by the dialect frontend) so this function contains no dialect-specific logic.
/// Falls back to dynamic expansion when native SQL is unavailable or not requested.
pub fn resolve_list_strategy(strategy: &ListParamStrategy, lp: &Parameter) -> ListAction {
    if *strategy == ListParamStrategy::Native {
        if let (Some(native_sql), Some(bind)) = (&lp.native_list_sql, &lp.native_list_bind) {
            let sql = rewrite_to_anon_params(native_sql);
            return match bind {
                NativeListBind::Array => ListAction::PgNative(sql),
                NativeListBind::Json => ListAction::JsonNative(sql),
            };
        }
    }
    ListAction::Dynamic
}

/// Shared logic for emitting dynamic `IN (?,?,…,?)` list-param bind calls.
///
/// Splits scalar parameters into before/after the list param by index order and
/// emits the appropriate JDBC bind calls. `se` is the statement-end string.
pub fn emit_dynamic_binds(
    src: &mut String,
    query: &Query,
    lp: &Parameter,
    se: &str,
    emit_foreach: &dyn Fn(&mut String, &str, usize, &str) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();
    let before_scalars: Vec<_> = scalar_params.iter().filter(|p| p.index < lp.index).collect();
    let after_scalars: Vec<_> = scalar_params.iter().filter(|p| p.index > lp.index).collect();

    for (i, sp) in before_scalars.iter().enumerate() {
        let idx = i + 1;
        if sp.nullable {
            writeln!(src, "            ps.setObject({idx}, {}){se}", to_camel_case(&sp.name))?;
        } else {
            writeln!(src, "            ps.{}({idx}, {}){se}", jdbc_setter(&sp.sql_type), to_camel_case(&sp.name))?;
        }
    }

    let base = before_scalars.len();
    let lp_name = to_camel_case(&lp.name);
    emit_foreach(src, &lp_name, base, jdbc_setter(&lp.sql_type))?;

    for (i, sp) in after_scalars.iter().enumerate() {
        let name = to_camel_case(&sp.name);
        if sp.nullable {
            writeln!(src, "            ps.setObject({} + {lp_name}.size{} + {}, {name}){se}", base, if se == ";" { "()" } else { "" }, i + 1)?;
        } else {
            writeln!(
                src,
                "            ps.{}({} + {lp_name}.size{} + {}, {name}){se}",
                jdbc_setter(&sp.sql_type),
                base,
                if se == ";" { "()" } else { "" },
                i + 1
            )?;
        }
    }
    Ok(())
}

/// Build the SQL constant and raw SQL for a query.
///
/// Returns `(const_name, raw_sql)` where `raw_sql` has placeholders rewritten for JDBC.
/// Callers are responsible for any escaping needed by their string literal syntax.
pub fn prepare_sql_const(query: &Query) -> (String, String) {
    let sql = rewrite_to_anon_params(&query.sql);
    (sql_const_name(&query.name), sql)
}

/// Build the SQL constant and raw SQL for a query with a pre-rewritten SQL.
///
/// Same as [`prepare_sql_const`] but starts from an already-rewritten SQL string
/// (used by list-param strategies that modify the SQL before rewriting placeholders).
pub fn prepare_sql_const_from(query: &Query, rewritten_sql: &str) -> (String, String) {
    let sql = rewrite_to_anon_params(rewritten_sql);
    (sql_const_name(&query.name), sql)
}

/// Escape a SQL string for embedding in a triple-quoted string literal.
///
/// Replaces `"""` with `\"""` to prevent premature termination of the text block.
/// Used by the Java and Kotlin backends.
pub(crate) fn escape_sql_triple_quoted(sql: &str) -> String {
    sql.replace("\"\"\"", "\\\"\"\"")
}

/// Build the `(before_esc, after_esc)` pair for dynamic IN expansion.
///
/// Splits the SQL at the list param's `IN ($N)` clause and escapes both halves.
pub fn prepare_dynamic_sql_parts(query: &Query, lp: &Parameter) -> (String, String) {
    let (before_raw, after_raw) = split_at_in_clause(&query.sql, lp.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
    (escape_sql(&rewrite_to_anon_params(&before_raw)), escape_sql(&rewrite_to_anon_params(&after_raw)))
}

/// Build a row constructor expression: `ClassName(arg1, arg2, …)`.
///
/// `prefix` is `"new "` for Java, `""` for Kotlin. `read_expr` is a closure that
/// maps `(sql_type, nullable, column_index)` to a driver read expression string,
/// allowing callers to inject type-override logic via a captured config reference.
/// Returns true for SQL types whose default JDBC read is `rs.getObject(idx, T.class)`.
///
/// When overriding one of these types, the generated `getObject` call must use the
/// override class name rather than the hardcoded default.
pub fn uses_get_object(sql_type: &SqlType) -> bool {
    matches!(sql_type, SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz | SqlType::Uuid)
}

pub fn build_row_constructor<F>(query: &Query, schema: &Schema, fallback: &str, prefix: &str, read_expr: F) -> String
where
    F: Fn(&SqlType, bool, usize) -> String,
{
    let class = result_row_type(query, schema, fallback);
    let args: Vec<String> = query.result_columns.iter().enumerate().map(|(i, col)| read_expr(&col.sql_type, col.nullable, i + 1)).collect();
    format!("{prefix}{class}({})", args.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Parameter, Query, ResultColumn, Schema, SqlType, Table};

    fn make_query(name: &str, sql: &str, params: Vec<Parameter>) -> Query {
        Query::one(name, sql, params, vec![])
    }

    fn make_schema() -> Schema {
        Schema {
            tables: vec![Table::new(
                "users",
                vec![crate::ir::Column::new_primary_key("id", SqlType::BigInt), crate::ir::Column::new_not_nullable("name", SqlType::Text)],
            )],
            ..Default::default()
        }
    }

    #[test]
    fn test_escape_sql_replaces_newlines_and_quotes() {
        assert_eq!(escape_sql("SELECT\n\"foo\""), r#"SELECT \"foo\""#);
    }

    #[test]
    fn test_result_row_type_uses_fallback() {
        let q = make_query("DeleteUser", "DELETE FROM users WHERE id = $1", vec![]);
        let schema = Schema::default();
        assert_eq!(result_row_type(&q, &schema, "Object[]"), "Object[]");
        assert_eq!(result_row_type(&q, &schema, "Any"), "Any");
    }

    #[test]
    fn test_result_row_type_infers_table() {
        let q = Query::one(
            "GetUser",
            "SELECT id, name FROM users WHERE id = $1",
            vec![],
            vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("name", SqlType::Text)],
        );
        assert_eq!(result_row_type(&q, &make_schema(), "Object[]"), "Users");
    }

    #[test]
    fn test_ds_result_row_type_qualifies_inline_rows() {
        let q = Query::one("GetSummary", "SELECT count(*) as cnt FROM users", vec![], vec![ResultColumn::not_nullable("cnt", SqlType::BigInt)]);
        let schema = Schema::default();
        assert_eq!(ds_result_row_type(&q, &schema, "Object[]", "Queries"), "Queries.GetSummaryRow");
    }

    #[test]
    fn test_emit_jdbc_binds_scalar_params() {
        let p1 = Parameter::scalar(1, "user_id", SqlType::BigInt, false);
        let p2 = Parameter::scalar(2, "name", SqlType::Text, true);
        let q = make_query("Test", "WHERE id = $1 AND name = $2", vec![p1, p2]);

        let mut src = String::new();
        emit_jdbc_binds(&mut src, &q, "", ";", "toArray()", |p| to_camel_case(&p.name)).unwrap();
        assert!(src.contains("ps.setLong(1, userId);"));
        assert!(src.contains("ps.setObject(2, name);"));

        let mut src = String::new();
        emit_jdbc_binds(&mut src, &q, "", "", "toTypedArray()", |p| to_camel_case(&p.name)).unwrap();
        assert!(src.contains("ps.setLong(1, userId)"));
        assert!(!src.contains("ps.setLong(1, userId);"));
    }

    #[test]
    fn test_emit_jdbc_binds_array_param_java_uses_to_array() {
        let p = Parameter::scalar(1, "tags", SqlType::Array(Box::new(SqlType::Text)), false);
        let q = make_query("UpdateTags", "UPDATE t SET tags = $1", vec![p]);

        let mut src = String::new();
        emit_jdbc_binds(&mut src, &q, "", ";", "toArray()", |p| to_camel_case(&p.name)).unwrap();
        assert!(src.contains("createArrayOf(\"text\", tags.toArray())"), "Java should use toArray(): {src}");
        assert!(src.contains("ps.setArray(1,"), "should use setArray, not setObject: {src}");
    }

    #[test]
    fn test_emit_jdbc_binds_array_param_kotlin_uses_to_typed_array() {
        // Regression: Kotlin generated `tags.toArray()` which is Java syntax and
        // does not compile under the Kotlin compiler. Kotlin requires `toTypedArray()`.
        let p = Parameter::scalar(1, "tags", SqlType::Array(Box::new(SqlType::Text)), false);
        let q = make_query("UpdateTags", "UPDATE t SET tags = $1", vec![p]);

        let mut src = String::new();
        emit_jdbc_binds(&mut src, &q, "", "", "toTypedArray()", |p| to_camel_case(&p.name)).unwrap();
        assert!(src.contains("createArrayOf(\"text\", tags.toTypedArray())"), "Kotlin should use toTypedArray(): {src}");
        assert!(!src.contains(".toArray()"), "Kotlin must not emit Java toArray(): {src}");
    }

    #[test]
    fn test_emit_jdbc_binds_json_param_uses_types_other() {
        let p = Parameter::scalar(1, "metadata", SqlType::Jsonb, false);
        let q = make_query("UpdateMeta", "UPDATE t SET meta = $1", vec![p]);

        let mut src = String::new();
        emit_jdbc_binds(&mut src, &q, "", ";", "toArray()", |p| to_camel_case(&p.name)).unwrap();
        assert!(src.contains("java.sql.Types.OTHER"), "JSONB should use Types.OTHER: {src}");
        assert!(src.contains("ps.setObject(1, metadata, java.sql.Types.OTHER)"), "full setObject call: {src}");
    }

    #[test]
    fn test_emit_jdbc_binds_json_plain_also_uses_types_other() {
        let p = Parameter::scalar(1, "data", SqlType::Json, false);
        let q = make_query("SetData", "UPDATE t SET data = $1", vec![p]);

        let mut src = String::new();
        emit_jdbc_binds(&mut src, &q, "", ";", "toArray()", |p| to_camel_case(&p.name)).unwrap();
        assert!(src.contains("java.sql.Types.OTHER"), "JSON should use Types.OTHER: {src}");
    }

    #[test]
    fn test_prepare_sql_const() {
        let q = make_query("GetUser", "SELECT * FROM users WHERE id = $1", vec![]);
        let (name, sql) = prepare_sql_const(&q);
        assert_eq!(name, "SQL_GET_USER");
        // Returns raw SQL (placeholder rewritten, no string-literal escaping)
        assert_eq!(sql, "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn test_resolve_list_strategy_postgres_native() {
        let q = make_query(
            "GetUsers",
            "SELECT * FROM users WHERE id IN ($1)",
            vec![Parameter::list(1, "ids", SqlType::BigInt, false).with_native_list("SELECT * FROM users WHERE id = ANY($1)", NativeListBind::Array)],
        );
        let lp = &q.params[0];
        match resolve_list_strategy(&ListParamStrategy::Native, lp) {
            ListAction::PgNative(sql) => assert!(sql.contains("= ANY(")),
            other => panic!("expected PgNative, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn test_resolve_list_strategy_dynamic() {
        let q = make_query("GetUsers", "SELECT * FROM users WHERE id IN ($1)", vec![Parameter::list(1, "ids", SqlType::BigInt, false)]);
        let lp = &q.params[0];
        assert!(matches!(resolve_list_strategy(&ListParamStrategy::Dynamic, lp), ListAction::Dynamic));
    }

    #[test]
    fn test_prepare_dynamic_sql_parts() {
        let q = make_query(
            "GetUsers",
            "SELECT * FROM users WHERE id IN ($1) AND active = $2",
            vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
        );
        let (before, after) = prepare_dynamic_sql_parts(&q, &q.params[0]);
        assert_eq!(before, "SELECT * FROM users WHERE id ");
        assert!(after.contains("AND active = ?"));
    }
}
