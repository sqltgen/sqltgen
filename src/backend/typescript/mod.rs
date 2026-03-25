use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_camel_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;

/// Database engine and npm driver to target for JS/TS output.
#[derive(Clone, Copy)]
pub enum JsTarget {
    /// PostgreSQL via node-postgres (`pg`).
    Postgres,
    /// SQLite via better-sqlite3.
    Sqlite,
    /// MySQL via mysql2.
    Mysql,
}

impl From<Engine> for JsTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => JsTarget::Postgres,
            Engine::Sqlite => JsTarget::Sqlite,
            Engine::Mysql => JsTarget::Mysql,
        }
    }
}

impl JsTarget {
    fn engine_str(&self) -> &'static str {
        match self {
            JsTarget::Postgres => "postgresql",
            JsTarget::Sqlite => "sqlite",
            JsTarget::Mysql => "mysql",
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
