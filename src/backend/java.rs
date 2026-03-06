use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, infer_table, jdbc_sql, sql_const_name, to_camel_case, to_pascal_case,
};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType};

pub struct JavaCodegen;

impl Codegen for JavaCodegen {
    fn generate(
        &self,
        schema: &Schema,
        queries: &[Query],
        config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One record class per table
        for table in &schema.tables {
            let class_name = to_pascal_case(&table.name);
            let mut src = String::new();
            emit_package(&mut src, &config.package, ";");
            writeln!(src, "public record {class_name}(")?;
            let params: Vec<String> = table.columns.iter().map(|col| {
                let ty = java_type(&col.sql_type, col.nullable);
                format!("    {} {}", ty, to_camel_case(&col.name))
            }).collect();
            writeln!(src, "{}", params.join(",\n"))?;
            writeln!(src, ") {{}}")?;

            let path = record_path(&config.out, &config.package, &class_name);
            files.push(GeneratedFile { path, content: src });
        }

        // One Queries class with static methods
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

            for query in queries {
                writeln!(src)?;
                if infer_table(query, schema).is_none() && !query.result_columns.is_empty() {
                    emit_row_record(&mut src, query)?;
                    writeln!(src)?;
                }
                emit_java_query(&mut src, query, schema)?;
            }

            writeln!(src, "}}")?;

            let path = record_path(&config.out, &config.package, "Queries");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_java_query(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let sql_const = sql_const_name(&query.name);
    let sql = jdbc_sql(&query.sql);
    writeln!(src, "    private static final String {sql_const} =")?;
    writeln!(src, "        \"{};\";", sql.replace('\n', " ").replace('"', "\\\""))?;

    let return_type = match query.cmd {
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            format!("Optional<{row_type}>")
        }
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            format!("List<{row_type}>")
        }
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };

    let params_sig: String = std::iter::once("Connection conn".to_string())
        .chain(query.params.iter().map(|p| {
            format!("{} {}", java_type(&p.sql_type, p.nullable), to_camel_case(&p.name))
        }))
        .collect::<Vec<_>>()
        .join(", ");

    writeln!(src, "    public static {return_type} {}({params_sig}) throws SQLException {{", to_camel_case(&query.name))?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;

    for p in &query.params {
        if p.nullable {
            writeln!(src, "            ps.setObject({}, {});", p.index, to_camel_case(&p.name))?;
        } else {
            let setter = jdbc_setter(&p.sql_type);
            writeln!(src, "            ps.{setter}({}, {});", p.index, to_camel_case(&p.name))?;
        }
    }

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "            ps.executeUpdate();")?;
        }
        QueryCmd::ExecRows => {
            writeln!(src, "            return ps.executeUpdate();")?;
        }
        QueryCmd::One => {
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                if (!rs.next()) return Optional.empty();")?;
            writeln!(src, "                return Optional.of({});", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
        }
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            writeln!(src, "            List<{row_type}> rows = new ArrayList<>();")?;
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                while (rs.next()) rows.add({});", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows;")?;
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
        return row_record_name(&query.name);
    }
    "Object[]".to_string()
}

fn row_record_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

fn emit_row_record(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = row_record_name(&query.name);
    writeln!(src, "    public record {name}(")?;
    let fields: Vec<String> = query.result_columns.iter().map(|col| {
        format!("        {} {}", java_type(&col.sql_type, col.nullable), to_camel_case(&col.name))
    }).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    ) {{}}")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema) -> String {
    let class = result_row_type(query, schema);
    let args: Vec<String> = query.result_columns.iter().enumerate().map(|(i, col)| {
        rs_read_expr(&col.sql_type, i + 1)
    }).collect();
    format!("new {class}({})", args.join(", "))
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

pub fn java_type(sql_type: &SqlType, nullable: bool) -> String {
    match sql_type {
        SqlType::Boolean   => if nullable { "Boolean".into()  } else { "boolean".into() },
        SqlType::SmallInt  => if nullable { "Short".into()    } else { "short".into() },
        SqlType::Integer   => if nullable { "Integer".into()  } else { "int".into() },
        SqlType::BigInt    => if nullable { "Long".into()     } else { "long".into() },
        SqlType::Real      => if nullable { "Float".into()    } else { "float".into() },
        SqlType::Double    => if nullable { "Double".into()   } else { "double".into() },
        SqlType::Decimal   => "java.math.BigDecimal".into(),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_)
                           => "String".into(),
        SqlType::Bytes     => "byte[]".into(),
        SqlType::Date      => "java.time.LocalDate".into(),
        SqlType::Time      => "java.time.LocalTime".into(),
        SqlType::Timestamp => "java.time.LocalDateTime".into(),
        SqlType::TimestampTz => "java.time.OffsetDateTime".into(),
        SqlType::Interval  => "String".into(),
        SqlType::Uuid      => "java.util.UUID".into(),
        SqlType::Json | SqlType::Jsonb => "String".into(),
        SqlType::Array(inner) => format!("java.util.List<{}>", java_type_boxed(inner)),
        SqlType::Custom(_) => "Object".into(),
    }
}

fn java_type_boxed(sql_type: &SqlType) -> String {
    match sql_type {
        SqlType::Boolean  => "Boolean".into(),
        SqlType::SmallInt => "Short".into(),
        SqlType::Integer  => "Integer".into(),
        SqlType::BigInt   => "Long".into(),
        SqlType::Real     => "Float".into(),
        SqlType::Double   => "Double".into(),
        other => java_type(other, false),
    }
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
        SqlType::Date                  => "setObject",
        SqlType::Time                  => "setObject",
        SqlType::Timestamp             => "setObject",
        SqlType::TimestampTz           => "setObject",
        SqlType::Uuid                  => "setObject",
        _                              => "setObject",
    }
}

fn rs_read_expr(sql_type: &SqlType, idx: usize) -> String {
    match sql_type {
        SqlType::Boolean    => format!("rs.getBoolean({idx})"),
        SqlType::SmallInt   => format!("rs.getShort({idx})"),
        SqlType::Integer    => format!("rs.getInt({idx})"),
        SqlType::BigInt     => format!("rs.getLong({idx})"),
        SqlType::Real       => format!("rs.getFloat({idx})"),
        SqlType::Double     => format!("rs.getDouble({idx})"),
        SqlType::Decimal    => format!("rs.getBigDecimal({idx})"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => format!("rs.getString({idx})"),
        SqlType::Bytes      => format!("rs.getBytes({idx})"),
        SqlType::Date       => format!("rs.getObject({idx}, java.time.LocalDate.class)"),
        SqlType::Time       => format!("rs.getObject({idx}, java.time.LocalTime.class)"),
        SqlType::Timestamp  => format!("rs.getObject({idx}, java.time.LocalDateTime.class)"),
        SqlType::TimestampTz => format!("rs.getObject({idx}, java.time.OffsetDateTime.class)"),
        SqlType::Uuid       => format!("rs.getObject({idx}, java.util.UUID.class)"),
        _                   => format!("rs.getObject({idx})"),
    }
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn record_path(out: &str, package: &str, class_name: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{class_name}.java"))
}
