use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType};

pub struct KotlinCodegen;

impl Codegen for KotlinCodegen {
    fn generate(
        &self,
        schema: &Schema,
        queries: &[Query],
        config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One data class per table
        for table in &schema.tables {
            let class_name = to_pascal_case(&table.name);
            let mut src = String::new();
            emit_package(&mut src, &config.package);
            writeln!(src, "data class {class_name}(")?;
            let params: Vec<String> = table.columns.iter().map(|col| {
                let ty = kotlin_type(&col.sql_type, col.nullable);
                format!("    val {}: {}", to_camel_case(&col.name), ty)
            }).collect();
            writeln!(src, "{}", params.join(",\n"))?;
            writeln!(src, ")")?;

            let path = source_path(&config.out, &config.package, &class_name, "kt");
            files.push(GeneratedFile { path, content: src });
        }

        // One Queries object
        if !queries.is_empty() {
            let mut src = String::new();
            emit_package(&mut src, &config.package);
            writeln!(src, "import java.sql.Connection")?;
            writeln!(src)?;
            writeln!(src, "object Queries {{")?;

            for query in queries {
                writeln!(src)?;
                if infer_table(query, schema).is_none() && !query.result_columns.is_empty() {
                    emit_row_class(&mut src, query)?;
                    writeln!(src)?;
                }
                emit_kotlin_query(&mut src, query, schema)?;
            }

            writeln!(src, "}}")?;

            let path = source_path(&config.out, &config.package, "Queries", "kt");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_kotlin_query(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let sql_const = format!("SQL_{}", query.name.to_uppercase());
    let sql = jdbc_sql(&query.sql);
    writeln!(src, "    private const val {sql_const} = \"{};\"", sql.replace('\n', " ").replace('"', "\\\""))?;

    let return_type = match query.cmd {
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            format!("{row_type}?")
        }
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            format!("List<{row_type}>")
        }
        QueryCmd::Exec => "Unit".to_string(),
        QueryCmd::ExecRows => "Long".to_string(),
    };

    let params_sig: String = std::iter::once("conn: Connection".to_string())
        .chain(query.params.iter().map(|p| {
            format!("{}: {}", to_camel_case(&p.name), kotlin_type(&p.sql_type, p.nullable))
        }))
        .collect::<Vec<_>>()
        .join(", ");

    writeln!(src, "    fun {}({params_sig}): {return_type} {{", to_camel_case(&query.name))?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;

    for p in &query.params {
        let setter = jdbc_setter(&p.sql_type);
        writeln!(src, "            ps.{setter}({}, {})", p.index, to_camel_case(&p.name))?;
    }

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "            ps.executeUpdate()")?;
        }
        QueryCmd::ExecRows => {
            writeln!(src, "            return ps.executeUpdate().toLong()")?;
        }
        QueryCmd::One => {
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                if (!rs.next()) return null")?;
            writeln!(src, "                return {}", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
        }
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            writeln!(src, "            val rows = mutableListOf<{row_type}>()")?;
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                while (rs.next()) rows.add({})", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows")?;
        }
    }

    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema) -> String {
    if let Some(table_name) = infer_table(query, schema) {
        return to_pascal_case(table_name);
    }
    if !query.result_columns.is_empty() {
        return row_class_name(&query.name);
    }
    "Any".to_string()
}

fn row_class_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

fn emit_row_class(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = row_class_name(&query.name);
    writeln!(src, "    data class {name}(")?;
    let fields: Vec<String> = query.result_columns.iter().map(|col| {
        format!("        val {}: {}", to_camel_case(&col.name), kotlin_type(&col.sql_type, col.nullable))
    }).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    )")?;
    Ok(())
}

fn infer_table<'a>(query: &Query, schema: &'a Schema) -> Option<&'a str> {
    for table in &schema.tables {
        if table.columns.len() == query.result_columns.len()
            && table.columns.iter().zip(&query.result_columns).all(|(a, b)| a.name == b.name)
        {
            return Some(&table.name);
        }
    }
    None
}

fn emit_row_constructor(query: &Query, schema: &Schema) -> String {
    let class = result_row_type(query, schema);
    let args: Vec<String> = query.result_columns.iter().enumerate().map(|(i, col)| {
        format!("rs.{}({})", rs_getter(&col.sql_type), i + 1)
    }).collect();
    format!("{class}({})", args.join(", "))
}

fn jdbc_sql(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek().map_or(false, |c| c.is_ascii_digit()) {
            out.push('?');
            while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                chars.next();
            }
        } else {
            out.push(ch);
        }
    }
    out
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

pub fn kotlin_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean   => "Boolean",
        SqlType::SmallInt  => "Short",
        SqlType::Integer   => "Int",
        SqlType::BigInt    => "Long",
        SqlType::Real      => "Float",
        SqlType::Double    => "Double",
        SqlType::Decimal   => "java.math.BigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "String",
        SqlType::Bytes     => "ByteArray",
        SqlType::Date      => "java.time.LocalDate",
        SqlType::Time      => "java.time.LocalTime",
        SqlType::Timestamp => "java.time.LocalDateTime",
        SqlType::TimestampTz => "java.time.OffsetDateTime",
        SqlType::Interval  => "String",
        SqlType::Uuid      => "java.util.UUID",
        SqlType::Json | SqlType::Jsonb => "String",
        SqlType::Array(inner) => return format!("List<{}>", kotlin_type(inner, false)),
        SqlType::Custom(_) => "Any",
    };
    if nullable { format!("{base}?") } else { base.to_string() }
}

fn jdbc_setter(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean               => "setBoolean",
        SqlType::SmallInt              => "setShort",
        SqlType::Integer               => "setInt",
        SqlType::BigInt                => "setLong",
        SqlType::Real                  => "setFloat",
        SqlType::Double                => "setDouble",
        SqlType::Decimal               => "setBigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "setString",
        SqlType::Bytes                 => "setBytes",
        _                              => "setObject",
    }
}

fn rs_getter(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean               => "getBoolean",
        SqlType::SmallInt              => "getShort",
        SqlType::Integer               => "getInt",
        SqlType::BigInt                => "getLong",
        SqlType::Real                  => "getFloat",
        SqlType::Double                => "getDouble",
        SqlType::Decimal               => "getBigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "getString",
        SqlType::Bytes                 => "getBytes",
        _                              => "getObject",
    }
}

// ─── Name helpers ─────────────────────────────────────────────────────────────

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect()
}

fn to_camel_case(s: &str) -> String {
    let pascal = to_pascal_case(s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
    }
}

fn emit_package(src: &mut String, package: &str) {
    if !package.is_empty() {
        writeln!(src, "package {package}").unwrap();
        writeln!(src).unwrap();
    }
}

fn source_path(out: &str, package: &str, name: &str, ext: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{name}.{ext}"))
}
