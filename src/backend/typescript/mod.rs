use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_camel_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;

/// npm driver to target for JS/TS output.
#[derive(Clone, Copy)]
pub enum JsTarget {
    /// PostgreSQL via node-postgres (`pg`).
    Pg,
    /// SQLite via better-sqlite3.
    BetterSqlite3,
    /// MySQL via mysql2.
    Mysql2,
}

impl JsTarget {
    /// Resolve the target from an engine and optional driver string.
    ///
    /// `driver: None` selects the default for the engine. An explicit driver name
    /// must match a supported driver for that engine; anything else is a fatal error.
    pub fn from_engine_and_driver(engine: Engine, driver: Option<&str>) -> anyhow::Result<Self> {
        match (engine, driver) {
            (Engine::Postgresql, None | Some("pg")) => Ok(JsTarget::Pg),
            (Engine::Sqlite, None | Some("better-sqlite3")) => Ok(JsTarget::BetterSqlite3),
            (Engine::Mysql, None | Some("mysql2")) => Ok(JsTarget::Mysql2),
            (_, Some(d)) => anyhow::bail!(
                "driver {:?} is not supported for typescript/{}; supported drivers: {}",
                d,
                engine.as_str(),
                Self::supported_drivers(engine),
            ),
        }
    }

    fn supported_drivers(engine: Engine) -> &'static str {
        match engine {
            Engine::Postgresql => "pg",
            Engine::Sqlite => "better-sqlite3",
            Engine::Mysql => "mysql2",
        }
    }

    fn engine_str(&self) -> &'static str {
        match self {
            JsTarget::Pg => "postgresql",
            JsTarget::BetterSqlite3 => "sqlite",
            JsTarget::Mysql2 => "mysql",
        }
    }
}

/// Whether to emit TypeScript (inline types) or JavaScript (JSDoc annotations).
#[derive(Clone, Copy)]
pub enum JsOutput {
    TypeScript,
    JavaScript,
}

/// Code generator for the `typescript` and `javascript` outputs.
pub struct TypeScriptCodegen {
    /// Database driver target.
    pub target: JsTarget,
    /// TypeScript or JavaScript output mode.
    pub output: JsOutput,
}

impl Codegen for TypeScriptCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_ts_contract(self.target, self.output);
        let ext = match self.output {
            JsOutput::TypeScript => "ts",
            JsOutput::JavaScript => "js",
        };

        let lang = match self.output {
            JsOutput::TypeScript => "typescript",
            JsOutput::JavaScript => "javascript",
        };

        let mut files = Vec::new();
        files.push(adapter::emit_helper_file(&contract, config, ext));
        files.extend(core::generate_core_files(self, schema, queries, &contract, config)?);

        if let Some(manifest) = build_manifest_file(
            lang,
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_camel_case,
            &|st, nullable| core::js_type_resolved(st, nullable, &contract, config),
            &|p| {
                if p.is_list {
                    let elem = core::js_type_resolved(&p.sql_type, false, &contract, config);
                    format!("{elem}[]")
                } else {
                    core::js_type_resolved(&p.sql_type, p.nullable, &contract, config)
                }
            },
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn js_type(sql_type: &SqlType, nullable: bool, target: &JsTarget) -> String {
    core::js_type(sql_type, nullable, target)
}

#[cfg(test)]
fn build_queries_file(group: &str, queries: &[Query], schema: &Schema, target: &JsTarget, output: &JsOutput, config: &OutputConfig) -> anyhow::Result<String> {
    core::build_queries_file(group, queries, schema, target, output, config)
}

#[cfg(test)]
fn emit_inline_row_type(src: &mut String, query: &Query, output: &JsOutput, target: &JsTarget, config: &OutputConfig) -> anyhow::Result<()> {
    core::emit_inline_row_type(src, query, output, target, config)
}

#[cfg(test)]
mod tests;
