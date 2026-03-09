use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, has_inline_rows, infer_row_type_name, infer_table, jdbc_bind_sequence, jdbc_setter, mysql_json_table_col_type, needs_null_safe_getter,
    pg_array_type_name, replace_list_in_clause, rewrite_to_anon_params, split_at_in_clause, sql_const_name, to_camel_case, to_pascal_case,
};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

/// Database engine target for the Java backend.
pub enum JavaTarget {
    Postgres,
    Mysql,
    Sqlite,
}

pub struct JavaCodegen {
    pub target: JavaTarget,
}

impl Codegen for JavaCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One record class per table
        for table in &schema.tables {
            let class_name = to_pascal_case(&table.name);
            let mut src = String::new();
            emit_package(&mut src, &config.package, ";");
            writeln!(src, "public record {class_name}(")?;
            let params: Vec<String> = table
                .columns
                .iter()
                .map(|col| {
                    let ty = java_type(&col.sql_type, col.nullable);
                    format!("    {} {}", ty, to_camel_case(&col.name))
                })
                .collect();
            writeln!(src, "{}", params.join(",\n"))?;
            writeln!(src, ") {{}}")?;

            let path = record_path(&config.out, &config.package, &class_name);
            files.push(GeneratedFile { path, content: src });
        }

        // One Queries class with static methods + one QueriesDs class backed by DataSource
        if !queries.is_empty() {
            let mut src = String::new();
            emit_package(&mut src, &config.package, ";");
            writeln!(src, "import java.sql.Connection;")?;
            writeln!(src, "import java.sql.PreparedStatement;")?;
            writeln!(src, "import java.sql.ResultSet;")?;
            writeln!(src, "import java.sql.SQLException;")?;
            writeln!(src, "import java.util.ArrayList;")?;
            writeln!(src, "import java.util.List;")?;
            writeln!(src, "import java.util.Optional;")?;
            writeln!(src)?;
            writeln!(src, "public final class Queries {{")?;
            writeln!(src, "    private Queries() {{}}")?;

            let strategy = config.list_params.clone().unwrap_or_default();
            for query in queries {
                writeln!(src)?;
                if has_inline_rows(query, schema) {
                    emit_row_record(&mut src, query)?;
                    writeln!(src)?;
                }
                emit_java_query(&mut src, query, schema, &self.target, &strategy)?;
            }

            writeln!(src, "}}")?;

            let path = record_path(&config.out, &config.package, "Queries");
            files.push(GeneratedFile { path, content: src });

            let mut src = String::new();
            emit_package(&mut src, &config.package, ";");
            emit_java_queries_ds(&mut src, queries, schema)?;
            let path = record_path(&config.out, &config.package, "QueriesDs");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_java_query(src: &mut String, query: &Query, schema: &Schema, target: &JavaTarget, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    let return_type = match query.cmd {
        QueryCmd::One => format!("Optional<{}>", result_row_type(query, schema)),
        QueryCmd::Many => format!("List<{}>", result_row_type(query, schema)),
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };
    let params_sig: String = std::iter::once("Connection conn".to_string())
        .chain(query.params.iter().map(|p| format!("{} {}", java_param_type(p), to_camel_case(&p.name))))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        emit_java_list_query(src, query, schema, target, strategy, lp, &return_type, &params_sig)
    } else {
        emit_java_scalar_query(src, query, schema, &return_type, &params_sig)
    }
}

fn emit_java_scalar_query(src: &mut String, query: &Query, schema: &Schema, return_type: &str, params_sig: &str) -> anyhow::Result<()> {
    let sql_const = sql_const_name(&query.name);
    let sql = rewrite_to_anon_params(&query.sql);
    writeln!(src, "    private static final String {sql_const} =")?;
    writeln!(src, "        \"{};\";", sql.replace('\n', " ").replace('"', "\\\""))?;
    writeln!(src, "    public static {return_type} {}({params_sig}) throws SQLException {{", to_camel_case(&query.name))?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    for (jdbc_idx, p) in jdbc_bind_sequence(query) {
        if p.nullable {
            writeln!(src, "            ps.setObject({jdbc_idx}, {});", to_camel_case(&p.name))?;
        } else {
            writeln!(src, "            ps.{}({jdbc_idx}, {});", jdbc_setter(&p.sql_type), to_camel_case(&p.name))?;
        }
    }
    emit_java_result_block(src, query, schema)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_java_list_query(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    target: &JavaTarget,
    strategy: &ListParamStrategy,
    lp: &Parameter,
    return_type: &str,
    params_sig: &str,
) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();
    let method_name = to_camel_case(&query.name);

    match (target, strategy) {
        (JavaTarget::Postgres, ListParamStrategy::Native) => {
            let sql = rewrite_to_anon_params(&replace_list_in_clause(&query.sql, lp.index, "= ANY(?)").unwrap_or_else(|| query.sql.clone()));
            let sql_const = sql_const_name(&query.name);
            writeln!(src, "    private static final String {sql_const} =")?;
            writeln!(src, "        \"{};\";", sql.replace('\n', " ").replace('"', "\\\""))?;
            writeln!(src, "    public static {return_type} {method_name}({params_sig}) throws SQLException {{")?;
            let type_name = pg_array_type_name(&lp.sql_type);
            writeln!(src, "        java.sql.Array arr = conn.createArrayOf(\"{type_name}\", {lp_name}.toArray());")?;
            writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
            for (jdbc_idx, p) in jdbc_bind_sequence(query) {
                if p.is_list {
                    writeln!(src, "            ps.setArray({jdbc_idx}, arr);")?;
                } else if p.nullable {
                    writeln!(src, "            ps.setObject({jdbc_idx}, {});", to_camel_case(&p.name))?;
                } else {
                    writeln!(src, "            ps.{}({jdbc_idx}, {});", jdbc_setter(&p.sql_type), to_camel_case(&p.name))?;
                }
            }
            emit_java_result_block(src, query, schema)?;
            writeln!(src, "        }}")?;
            writeln!(src, "    }}")?;
        },
        (_, ListParamStrategy::Dynamic) => {
            let (before_raw, after_raw) = split_at_in_clause(&query.sql, lp.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
            let before_esc = rewrite_to_anon_params(&before_raw).replace('\n', " ").replace('"', "\\\"");
            let after_esc = rewrite_to_anon_params(&after_raw).replace('\n', " ").replace('"', "\\\"");
            writeln!(src, "    public static {return_type} {method_name}({params_sig}) throws SQLException {{")?;
            writeln!(src, "        String marks = {lp_name}.stream().map(x -> \"?\").collect(java.util.stream.Collectors.joining(\", \"));")?;
            writeln!(src, "        String sql = \"{before_esc}\" + \"IN (\" + marks + \"){after_esc};\";")?;
            writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement(sql)) {{")?;
            for (i, sp) in scalar_params.iter().enumerate() {
                let idx = i + 1;
                if sp.nullable {
                    writeln!(src, "            ps.setObject({idx}, {});", to_camel_case(&sp.name))?;
                } else {
                    writeln!(src, "            ps.{}({idx}, {});", jdbc_setter(&sp.sql_type), to_camel_case(&sp.name))?;
                }
            }
            let base = scalar_params.len();
            writeln!(src, "            for (int i = 0; i < {lp_name}.size(); i++) {{")?;
            writeln!(src, "                ps.{}({base} + i + 1, {lp_name}.get(i));", jdbc_setter(&lp.sql_type))?;
            writeln!(src, "            }}")?;
            emit_java_result_block(src, query, schema)?;
            writeln!(src, "        }}")?;
            writeln!(src, "    }}")?;
        },
        (JavaTarget::Sqlite, ListParamStrategy::Native) => {
            // SQLite: json_each(?) unpacks a JSON array string into rows.
            let repl = "IN (SELECT value FROM json_each(?))";
            let sql = rewrite_to_anon_params(&replace_list_in_clause(&query.sql, lp.index, repl).unwrap_or_else(|| query.sql.clone()));
            let sql_const = sql_const_name(&query.name);
            writeln!(src, "    private static final String {sql_const} =")?;
            writeln!(src, "        \"{};\";", sql.replace('\n', " ").replace('"', "\\\""))?;
            writeln!(src, "    public static {return_type} {method_name}({params_sig}) throws SQLException {{")?;
            writeln!(
                src,
                "        String json = \"[\" + {lp_name}.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(\",\")) + \"]\";"
            )?;
            writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
            for (jdbc_idx, p) in jdbc_bind_sequence(query) {
                if p.is_list {
                    writeln!(src, "            ps.setString({jdbc_idx}, json);")?;
                } else if p.nullable {
                    writeln!(src, "            ps.setObject({jdbc_idx}, {});", to_camel_case(&p.name))?;
                } else {
                    writeln!(src, "            ps.{}({jdbc_idx}, {});", jdbc_setter(&p.sql_type), to_camel_case(&p.name))?;
                }
            }
            emit_java_result_block(src, query, schema)?;
            writeln!(src, "        }}")?;
            writeln!(src, "    }}")?;
        },
        (JavaTarget::Mysql, ListParamStrategy::Native) => {
            let elem_type = mysql_json_table_col_type(&lp.sql_type);
            // Pass the full JSON array string as the JDBC ? — no CONCAT wrapping.
            let repl = format!("IN (SELECT value FROM JSON_TABLE(?,'$[*]' COLUMNS(value {elem_type} PATH '$')) t)");
            let sql = rewrite_to_anon_params(&replace_list_in_clause(&query.sql, lp.index, &repl).unwrap_or_else(|| query.sql.clone()));
            let sql_const = sql_const_name(&query.name);
            writeln!(src, "    private static final String {sql_const} =")?;
            writeln!(src, "        \"{};\";", sql.replace('\n', " ").replace('"', "\\\""))?;
            writeln!(src, "    public static {return_type} {method_name}({params_sig}) throws SQLException {{")?;
            writeln!(
                src,
                "        String json = \"[\" + {lp_name}.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(\",\")) + \"]\";"
            )?;
            writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
            for (jdbc_idx, p) in jdbc_bind_sequence(query) {
                if p.is_list {
                    writeln!(src, "            ps.setString({jdbc_idx}, json);")?;
                } else if p.nullable {
                    writeln!(src, "            ps.setObject({jdbc_idx}, {});", to_camel_case(&p.name))?;
                } else {
                    writeln!(src, "            ps.{}({jdbc_idx}, {});", jdbc_setter(&p.sql_type), to_camel_case(&p.name))?;
                }
            }
            emit_java_result_block(src, query, schema)?;
            writeln!(src, "        }}")?;
            writeln!(src, "    }}")?;
        },
    }
    Ok(())
}

/// Emit the result-reading block (executeUpdate / executeQuery / fetch loop).
fn emit_java_result_block(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    match query.cmd {
        QueryCmd::Exec => writeln!(src, "            ps.executeUpdate();")?,
        QueryCmd::ExecRows => writeln!(src, "            return ps.executeUpdate();")?,
        QueryCmd::One => {
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                if (!rs.next()) return Optional.empty();")?;
            writeln!(src, "                return Optional.of({});", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            writeln!(src, "            List<{row_type}> rows = new ArrayList<>();")?;
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                while (rs.next()) rows.add({});", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows;")?;
        },
    }
    Ok(())
}

/// Return the Java type for a parameter, using `List<T>` for list params.
fn java_param_type(p: &Parameter) -> String {
    if p.is_list {
        format!("List<{}>", java_type_boxed(&p.sql_type))
    } else {
        java_type(&p.sql_type, p.nullable)
    }
}

/// Emits `QueriesDs.java` — a DataSource-backed wrapper that acquires a connection
/// per call and delegates to the static methods in `Queries`.
fn emit_java_queries_ds(src: &mut String, queries: &[Query], schema: &Schema) -> anyhow::Result<()> {
    let has_one = queries.iter().any(|q| q.cmd == QueryCmd::One);
    let has_many = queries.iter().any(|q| q.cmd == QueryCmd::Many);

    writeln!(src, "import java.sql.Connection;")?;
    writeln!(src, "import java.sql.SQLException;")?;
    if has_many {
        writeln!(src, "import java.util.List;")?;
    }
    if has_one {
        writeln!(src, "import java.util.Optional;")?;
    }
    writeln!(src, "import javax.sql.DataSource;")?;
    writeln!(src)?;
    writeln!(src, "public final class QueriesDs {{")?;
    writeln!(src, "    private final DataSource dataSource;")?;
    writeln!(src)?;
    writeln!(src, "    public QueriesDs(DataSource dataSource) {{")?;
    writeln!(src, "        this.dataSource = dataSource;")?;
    writeln!(src, "    }}")?;

    for query in queries {
        writeln!(src)?;
        emit_java_ds_method(src, query, schema)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one instance method on `QueriesDs` that wraps the corresponding static `Queries` method.
fn emit_java_ds_method(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    // Inline row records are inner types of `Queries` and must be qualified.
    let row_type = ds_result_row_type(query, schema);
    let return_type = match query.cmd {
        QueryCmd::One => format!("Optional<{row_type}>"),
        QueryCmd::Many => format!("List<{row_type}>"),
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };

    let params_sig: String = query.params.iter().map(|p| format!("{} {}", java_param_type(p), to_camel_case(&p.name))).collect::<Vec<_>>().join(", ");

    let method_name = to_camel_case(&query.name);
    let args: String = query.params.iter().map(|p| to_camel_case(&p.name)).collect::<Vec<_>>().join(", ");
    let call_args = if args.is_empty() { "conn".to_string() } else { format!("conn, {args}") };

    writeln!(src, "    public {return_type} {method_name}({params_sig}) throws SQLException {{")?;
    writeln!(src, "        try (Connection conn = dataSource.getConnection()) {{")?;
    match query.cmd {
        QueryCmd::Exec => writeln!(src, "            Queries.{method_name}({call_args});")?,
        _ => writeln!(src, "            return Queries.{method_name}({call_args});")?,
    }
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema) -> String {
    infer_row_type_name(query, schema).unwrap_or_else(|| "Object[]".to_string())
}

/// Like [`result_row_type`], but qualifies inline row records as `Queries.XxxRow`
/// because they are inner types of `Queries` and must be fully referenced from `QueriesDs`.
fn ds_result_row_type(query: &Query, schema: &Schema) -> String {
    if let Some(table_name) = infer_table(query, schema) {
        return to_pascal_case(table_name);
    }
    if !query.result_columns.is_empty() {
        return format!("Queries.{}Row", to_pascal_case(&query.name));
    }
    "Object[]".to_string()
}

fn emit_row_record(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = format!("{}Row", to_pascal_case(&query.name));
    writeln!(src, "    public record {name}(")?;
    let fields: Vec<String> =
        query.result_columns.iter().map(|col| format!("        {} {}", java_type(&col.sql_type, col.nullable), to_camel_case(&col.name))).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    ) {{}}")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema) -> String {
    let class = result_row_type(query, schema);
    let args: Vec<String> = query.result_columns.iter().enumerate().map(|(i, col)| rs_read_expr(&col.sql_type, col.nullable, i + 1)).collect();
    format!("new {class}({})", args.join(", "))
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

pub fn java_type(sql_type: &SqlType, nullable: bool) -> String {
    match sql_type {
        SqlType::Boolean => {
            if nullable {
                "Boolean".into()
            } else {
                "boolean".into()
            }
        },
        SqlType::SmallInt => {
            if nullable {
                "Short".into()
            } else {
                "short".into()
            }
        },
        SqlType::Integer => {
            if nullable {
                "Integer".into()
            } else {
                "int".into()
            }
        },
        SqlType::BigInt => {
            if nullable {
                "Long".into()
            } else {
                "long".into()
            }
        },
        SqlType::Real => {
            if nullable {
                "Float".into()
            } else {
                "float".into()
            }
        },
        SqlType::Double => {
            if nullable {
                "Double".into()
            } else {
                "double".into()
            }
        },
        SqlType::Decimal => "java.math.BigDecimal".into(),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "String".into(),
        SqlType::Bytes => "byte[]".into(),
        SqlType::Date => "java.time.LocalDate".into(),
        SqlType::Time => "java.time.LocalTime".into(),
        SqlType::Timestamp => "java.time.LocalDateTime".into(),
        SqlType::TimestampTz => "java.time.OffsetDateTime".into(),
        SqlType::Interval => "String".into(),
        SqlType::Uuid => "java.util.UUID".into(),
        SqlType::Json | SqlType::Jsonb => "String".into(),
        SqlType::Array(inner) => {
            let t = format!("java.util.List<{}>", java_type_boxed(inner));
            if nullable {
                format!("@Nullable {t}")
            } else {
                t
            }
        },
        SqlType::Custom(_) => "Object".into(),
    }
}

fn java_type_boxed(sql_type: &SqlType) -> String {
    match sql_type {
        SqlType::Boolean => "Boolean".into(),
        SqlType::SmallInt => "Short".into(),
        SqlType::Integer => "Integer".into(),
        SqlType::BigInt => "Long".into(),
        SqlType::Real => "Float".into(),
        SqlType::Double => "Double".into(),
        other => java_type(other, false),
    }
}

fn rs_read_expr(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    // Primitive getters (getInt, getBoolean, …) return 0/false for SQL NULL.
    // For nullable primitive columns we must use getObject with the boxed type
    // so that the result can be null, matching the @Nullable field declaration.
    if nullable && needs_null_safe_getter(sql_type) {
        return match sql_type {
            SqlType::Boolean => format!("rs.getObject({idx}, Boolean.class)"),
            SqlType::SmallInt => format!("rs.getObject({idx}, Short.class)"),
            SqlType::Integer => format!("rs.getObject({idx}, Integer.class)"),
            SqlType::BigInt => format!("rs.getObject({idx}, Long.class)"),
            SqlType::Real => format!("rs.getObject({idx}, Float.class)"),
            SqlType::Double => format!("rs.getObject({idx}, Double.class)"),
            _ => unreachable!("needs_null_safe_getter returned true for non-primitive"),
        };
    }
    match sql_type {
        SqlType::Boolean => format!("rs.getBoolean({idx})"),
        SqlType::SmallInt => format!("rs.getShort({idx})"),
        SqlType::Integer => format!("rs.getInt({idx})"),
        SqlType::BigInt => format!("rs.getLong({idx})"),
        SqlType::Real => format!("rs.getFloat({idx})"),
        SqlType::Double => format!("rs.getDouble({idx})"),
        SqlType::Decimal => format!("rs.getBigDecimal({idx})"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => format!("rs.getString({idx})"),
        SqlType::Bytes => format!("rs.getBytes({idx})"),
        SqlType::Date => format!("rs.getObject({idx}, java.time.LocalDate.class)"),
        SqlType::Time => format!("rs.getObject({idx}, java.time.LocalTime.class)"),
        SqlType::Timestamp => format!("rs.getObject({idx}, java.time.LocalDateTime.class)"),
        SqlType::TimestampTz => format!("rs.getObject({idx}, java.time.OffsetDateTime.class)"),
        SqlType::Uuid => format!("rs.getObject({idx}, java.util.UUID.class)"),
        _ => format!("rs.getObject({idx})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OutputConfig;
    use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

    fn cfg() -> OutputConfig {
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: None }
    }

    fn cfg_pkg() -> OutputConfig {
        OutputConfig { out: "out".to_string(), package: "com.example.db".to_string(), list_params: None }
    }

    fn pg() -> JavaCodegen {
        JavaCodegen { target: JavaTarget::Postgres }
    }

    fn get_file<'a>(files: &'a [GeneratedFile], name: &str) -> &'a str {
        files.iter().find(|f| f.path.file_name().is_some_and(|n| n == name)).unwrap_or_else(|| panic!("file {name:?} not found")).content.as_str()
    }

    fn user_table() -> Table {
        Table {
            name: "user".to_string(),
            columns: vec![
                Column { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "name".to_string(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                Column { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true, is_primary_key: false },
            ],
        }
    }

    // ─── java_type ─────────────────────────────────────────────────────────

    #[test]
    fn test_java_type_boolean_non_nullable() {
        assert_eq!(java_type(&SqlType::Boolean, false), "boolean");
    }

    #[test]
    fn test_java_type_boolean_nullable() {
        assert_eq!(java_type(&SqlType::Boolean, true), "Boolean");
    }

    #[test]
    fn test_java_type_integer_non_nullable() {
        assert_eq!(java_type(&SqlType::Integer, false), "int");
    }

    #[test]
    fn test_java_type_integer_nullable() {
        assert_eq!(java_type(&SqlType::Integer, true), "Integer");
    }

    #[test]
    fn test_java_type_bigint_non_nullable() {
        assert_eq!(java_type(&SqlType::BigInt, false), "long");
    }

    #[test]
    fn test_java_type_bigint_nullable() {
        assert_eq!(java_type(&SqlType::BigInt, true), "Long");
    }

    #[test]
    fn test_java_type_text_ignores_nullability() {
        // String is a reference type — same in both cases
        assert_eq!(java_type(&SqlType::Text, false), "String");
        assert_eq!(java_type(&SqlType::Text, true), "String");
    }

    #[test]
    fn test_java_type_decimal() {
        assert_eq!(java_type(&SqlType::Decimal, false), "java.math.BigDecimal");
    }

    #[test]
    fn test_java_type_temporal() {
        assert_eq!(java_type(&SqlType::Date, false), "java.time.LocalDate");
        assert_eq!(java_type(&SqlType::Time, false), "java.time.LocalTime");
        assert_eq!(java_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
        assert_eq!(java_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
    }

    #[test]
    fn test_java_type_uuid() {
        assert_eq!(java_type(&SqlType::Uuid, false), "java.util.UUID");
    }

    #[test]
    fn test_java_type_json() {
        assert_eq!(java_type(&SqlType::Json, false), "String");
        assert_eq!(java_type(&SqlType::Jsonb, false), "String");
    }

    #[test]
    fn test_java_type_array_non_nullable() {
        assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), false), "java.util.List<String>");
    }

    #[test]
    fn test_java_type_array_nullable() {
        assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), true), "@Nullable java.util.List<String>");
    }

    #[test]
    fn test_java_type_array_of_integers_uses_boxed_type() {
        // Array elements must be boxed — List<int> is invalid Java
        assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "java.util.List<Integer>");
    }

    #[test]
    fn test_java_type_custom() {
        assert_eq!(java_type(&SqlType::Custom("citext".to_string()), false), "Object");
    }

    // ─── generate: table record ─────────────────────────────────────────────

    #[test]
    fn test_generate_table_record() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg()).unwrap();
        let src = get_file(&files, "User.java");
        assert!(src.contains("public record User("));
        assert!(src.contains("long id"));
        assert!(src.contains("String name"));
        assert!(src.contains("String bio"));
    }

    #[test]
    fn test_generate_package_declaration() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg_pkg()).unwrap();
        let src = get_file(&files, "User.java");
        assert!(src.contains("package com.example.db;"));
    }

    #[test]
    fn test_generate_no_queries_produces_no_queries_file() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg()).unwrap();
        assert_eq!(files.len(), 1);
    }

    // ─── generate: query commands ───────────────────────────────────────────

    #[test]
    fn test_generate_exec_query() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static void deleteUser(Connection conn, long id)"));
        assert!(src.contains("ps.executeUpdate();"));
    }

    #[test]
    fn test_generate_execrows_query() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUsers".to_string(),
            cmd: QueryCmd::ExecRows,
            sql: "DELETE FROM user WHERE active = $1".to_string(),
            params: vec![Parameter::scalar(1, "active".to_string(), SqlType::Boolean, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static long deleteUsers("));
        assert!(src.contains("return ps.executeUpdate();"));
    }

    #[test]
    fn test_generate_one_query_infers_table_return_type() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUser".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT id, name, bio FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static Optional<User> getUser("));
        assert!(src.contains("if (!rs.next()) return Optional.empty();"));
        assert!(src.contains("return Optional.of(new User("));
    }

    #[test]
    fn test_generate_many_query_infers_table_return_type() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "ListUsers".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id, name, bio FROM user".to_string(),
            params: vec![],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static List<User> listUsers(Connection conn)"));
        assert!(src.contains("while (rs.next()) rows.add(new User("));
        assert!(src.contains("return rows;"));
    }

    // ─── generate: SQL constant name ────────────────────────────────────────

    #[test]
    fn test_generate_sql_const_name_is_screaming_snake_case() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetUserById".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("SQL_GET_USER_BY_ID"));
    }

    // ─── generate: inline row record ────────────────────────────────────────

    #[test]
    fn test_generate_inline_row_record_for_partial_result() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUserName".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT name FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public record GetUserNameRow("));
        assert!(src.contains("Optional<GetUserNameRow>"));
    }

    // ─── generate: nullable result column uses getObject ────────────────────

    #[test]
    fn test_generate_nullable_integer_result_uses_get_object() {
        // rs.getInt returns 0 for NULL; nullable Integer columns must use getObject
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::Integer, nullable: true }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("rs.getObject(1, Integer.class)"));
        assert!(!src.contains("rs.getInt(1)"));
    }

    #[test]
    fn test_generate_non_nullable_integer_result_uses_get_int() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::Integer, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("rs.getInt(1)"));
    }

    // ─── generate: QueriesDs ────────────────────────────────────────────────

    #[test]
    fn test_generate_queries_ds_file_is_emitted() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        assert!(files.iter().any(|f| f.path.file_name().is_some_and(|n| n == "QueriesDs.java")));
    }

    #[test]
    fn test_generate_queries_ds_constructor_and_datasource_import() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.java");
        assert!(src.contains("import javax.sql.DataSource;"));
        assert!(src.contains("public final class QueriesDs {"));
        assert!(src.contains("private final DataSource dataSource;"));
        assert!(src.contains("public QueriesDs(DataSource dataSource)"));
    }

    #[test]
    fn test_generate_queries_ds_exec_method_delegates_to_queries() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.java");
        assert!(src.contains("public void deleteUser(long id) throws SQLException"));
        assert!(src.contains("try (Connection conn = dataSource.getConnection())"));
        assert!(src.contains("Queries.deleteUser(conn, id);"));
    }

    #[test]
    fn test_generate_queries_ds_one_method_returns_optional() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUser".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT id, name, bio FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.java");
        assert!(src.contains("import java.util.Optional;"));
        assert!(src.contains("public Optional<User> getUser(long id) throws SQLException"));
        assert!(src.contains("return Queries.getUser(conn, id);"));
    }

    #[test]
    fn test_generate_queries_ds_many_method_returns_list() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "ListUsers".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id, name, bio FROM user".to_string(),
            params: vec![],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.java");
        assert!(src.contains("import java.util.List;"));
        assert!(src.contains("public List<User> listUsers() throws SQLException"));
        assert!(src.contains("return Queries.listUsers(conn);"));
    }

    // ─── generate: repeated parameter binding ───────────────────────────────

    #[test]
    fn test_generate_repeated_param_emits_bind_per_occurrence() {
        // $1 appears 4 times, $2 once — must emit 5 bind calls in SQL order
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "FindItems".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT * FROM t WHERE a = $1 OR $1 = -1 AND b = $1 OR $1 = 0 AND c = $2".to_string(),
            params: vec![
                Parameter::scalar(1, "accountId".to_string(), SqlType::BigInt, false),
                Parameter::scalar(2, "inputData".to_string(), SqlType::Text, false),
            ],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        // Four bind calls for accountId (slots 1-4) and one for inputData (slot 5)
        assert!(src.contains("ps.setLong(1, accountId)"));
        assert!(src.contains("ps.setLong(2, accountId)"));
        assert!(src.contains("ps.setLong(3, accountId)"));
        assert!(src.contains("ps.setLong(4, accountId)"));
        assert!(src.contains("ps.setString(5, inputData)"));
        // Old (wrong) single binding must not appear
        assert!(!src.contains("ps.setString(2, inputData)") || src.contains("ps.setString(5, inputData)"));
    }

    // ─── generate: parameter binding ────────────────────────────────────────

    #[test]
    fn test_generate_nullable_param_uses_set_object() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "UpdateBio".to_string(),
            cmd: QueryCmd::Exec,
            sql: "UPDATE user SET bio = $1 WHERE id = $2".to_string(),
            params: vec![Parameter::scalar(1, "bio".to_string(), SqlType::Text, true), Parameter::scalar(2, "id".to_string(), SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("ps.setObject(1, bio)")); // nullable → setObject
        assert!(src.contains("ps.setLong(2, id)")); // non-nullable → typed setter
    }

    // ─── generate: list params ──────────────────────────────────────────────

    #[test]
    fn test_generate_pg_native_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("List<Long> ids"), "should use List<Long> for list param");
        assert!(src.contains("= ANY(?)"), "PG native should use ANY");
        assert!(src.contains("createArrayOf(\"bigint\""), "should call createArrayOf");
        assert!(src.contains("ps.setArray(1, arr)"), "should setArray");
    }

    #[test]
    fn test_generate_pg_dynamic_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
        let files = pg().generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("List<Long> ids"), "should use List<Long> for list param");
        assert!(src.contains("IN (\" + marks + \")"), "dynamic builds IN at runtime");
    }

    #[test]
    fn test_generate_sqlite_native_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = JavaCodegen { target: JavaTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
        assert!(!src.contains("JSON_TABLE"), "SQLite should not use MySQL JSON_TABLE");
        assert!(src.contains("ps.setString"), "should bind JSON string");
    }

    // ─── Bug A: JSON escaping for text list params in native strategy ────────────

    #[test]
    #[ignore = "exposes bug A (task 023): fix before enabling"]
    fn test_bug_a_sqlite_native_text_list_json_escaping() {
        // Bug A: The SQLite/MySQL native strategy uses Object::toString for all
        // element types. For Text params this produces bare unquoted strings —
        // invalid JSON. This test fails until the root cause is fixed.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByTags".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE tag IN ($1)".to_string(),
            params: vec![Parameter::list(1, "tags", SqlType::Text, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = JavaCodegen { target: JavaTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        // Object::toString on a String yields a bare value with no JSON quoting.
        assert!(!src.contains("Object::toString"), "text list must not use Object::toString (produces bare strings)");
        // The fix must wrap each element in \"...\" and escape special characters.
        assert!(src.contains(r#""\"" + x"#), "each text element must be wrapped in JSON quotes");
        assert!(src.contains(r#".replace("\\", "\\\\")"#), "backslashes in text values must be escaped");
    }

    #[test]
    fn test_bug_a_numeric_list_no_quoting_needed() {
        // Numeric types produce valid JSON via toString() — no per-element quoting
        // is needed. Confirm the fix does not introduce unnecessary quoting for
        // numeric list params.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = JavaCodegen { target: JavaTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
        assert!(!src.contains(r#""\"" + x"#), "numeric list must not add per-element quoting");
    }

    // ─── Bug B: dynamic strategy binds scalars at wrong slot when scalar follows IN

    #[test]
    #[ignore = "exposes bug B (task 023): fix before enabling"]
    fn test_bug_b_dynamic_scalar_after_in_binding_order() {
        // Bug B: when a scalar param appears *after* the IN clause in the SQL, the
        // Dynamic strategy incorrectly binds it at slot 1 (before list elements).
        // Correct order: [list elements] + [scalar-after].
        // This test fails until the root cause is fixed.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetActiveByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1) AND active = $2".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
        let files = JavaCodegen { target: JavaTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "Queries.java");
        // Bug: active is incorrectly bound at slot 1 before the list elements.
        assert!(!src.contains("ps.setBoolean(1, active)"), "active must not bind at slot 1 when it follows IN");
        // Fix: the list binding loop must appear before the scalar-after binding.
        let loop_pos = src.find("for (int i = 0; i <").expect("list binding loop not found");
        let active_pos = src.find("setBoolean").expect("active binding not found");
        assert!(loop_pos < active_pos, "list binding loop must precede the scalar-after binding");
        // Fix: slot for active depends on the runtime list size.
        assert!(src.contains("ids.size()"), "slot for active must be computed from ids.size() at runtime");
    }

    #[test]
    fn test_bug_b_dynamic_scalar_before_in_no_regression() {
        // When the scalar param appears *before* the IN clause, the current binding
        // order is correct. Confirm the fix preserves this common pattern.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetActiveByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE active = $1 AND id IN ($2)".to_string(),
            params: vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
        let files = JavaCodegen { target: JavaTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "Queries.java");
        // active is before IN in the SQL — must still bind at slot 1.
        assert!(src.contains("ps.setBoolean(1, active)"), "scalar before IN must bind at slot 1");
        // The scalar binding must precede the list loop.
        let active_pos = src.find("ps.setBoolean(1, active)").unwrap();
        let loop_pos = src.find("for (int i = 0; i <").expect("list binding loop not found");
        assert!(active_pos < loop_pos, "before-scalar binding must precede the list binding loop");
    }
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn record_path(out: &str, package: &str, class_name: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{class_name}.java"))
}
