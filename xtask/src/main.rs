mod collectors;
mod ratchet;
mod report;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::report::Report;

const REPORT_FILE: &str = "quality-report.json";

#[derive(Parser)]
#[command(name = "xtask", about = "Internal dev tools for sqltgen")]
struct Cli {
    #[command(subcommand)]
    command: TopCmd,
}

#[derive(Subcommand)]
enum TopCmd {
    /// Code quality snapshot and ratchet
    Quality {
        #[command(subcommand)]
        cmd: QualityCmd,
    },
}

#[derive(Subcommand)]
enum QualityCmd {
    /// Generate quality-report.json from the current codebase.
    Generate,
    /// Verify that the committed quality-report.json matches the current codebase.
    Check,
    /// Compare the committed quality-report.json against a baseline ref.
    Ratchet {
        /// Git ref to compare against (e.g. origin/main).
        #[arg(long, default_value = "origin/main")]
        base: String,
    },
    /// Debug: dump the FuncSpace tree for one file.
    Dump { path: PathBuf },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        TopCmd::Quality { cmd } => quality(cmd),
    }
}

fn quality(cmd: QualityCmd) -> Result<()> {
    let root = workspace_root()?;
    let report_path = root.join(REPORT_FILE);

    match cmd {
        QualityCmd::Generate => {
            let report = report::generate(&root)?;
            report.write_to(&report_path)?;
            println!("wrote {}", report_path.display());
        },
        QualityCmd::Check => {
            let actual = report::generate(&root)?;
            let committed = Report::read_from(&report_path).with_context(|| format!("reading {}", report_path.display()))?;
            if actual != committed {
                bail!(
                    "{} is out of date. Run `make quality-generate` and commit the result.\n\n\
                     diff (committed → regenerated):\n{}",
                    REPORT_FILE,
                    pretty_diff(&committed.to_pretty_string(), &actual.to_pretty_string()),
                );
            }
            println!("ok: {} matches the codebase", REPORT_FILE);
        },
        QualityCmd::Ratchet { base } => {
            let current = Report::read_from(&report_path).with_context(|| format!("reading {}", report_path.display()))?;
            match read_report_at_ref(&root, &base)? {
                None => {
                    eprintln!("warning: no {REPORT_FILE} at {base} — bootstrap mode, ratchet skipped");
                },
                Some(baseline) => {
                    if baseline.thresholds != current.thresholds {
                        bail!(
                            "thresholds differ between {base} and HEAD; threshold edits must \
                             land in their own PR. Revert the threshold change or split the PR."
                        );
                    }
                    let errors = ratchet::check(&baseline, &current);
                    if !errors.is_empty() {
                        bail!("ratchet violations:\n{}", ratchet::format_errors(&errors));
                    }
                },
            }
            println!("ok: ratchet check passed");
        },
        QualityCmd::Dump { path } => {
            collectors::structural::dump_tree(&path)?;
        },
    }
    Ok(())
}

fn workspace_root() -> Result<PathBuf> {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().map(Path::to_path_buf).context("xtask manifest has no parent")
}

fn read_report_at_ref(workspace_root: &Path, base: &str) -> Result<Option<Report>> {
    let spec = format!("{base}:{REPORT_FILE}");
    let out = Command::new("git").args(["show", &spec]).current_dir(workspace_root).output().context("running git show")?;
    if !out.status.success() {
        return Ok(None);
    }
    let report: Report = serde_json::from_slice(&out.stdout).with_context(|| format!("parsing report from `git show {spec}`"))?;
    Ok(Some(report))
}

fn pretty_diff(a: &str, b: &str) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let a_lines: Vec<&str> = a.lines().collect();
    let b_lines: Vec<&str> = b.lines().collect();
    let mut shown = 0;
    let max = a_lines.len().max(b_lines.len());
    for i in 0..max {
        let a_line = a_lines.get(i).copied();
        let b_line = b_lines.get(i).copied();
        if a_line == b_line {
            continue;
        }
        if let Some(a) = a_line {
            let _ = writeln!(out, "  - {a}");
            shown += 1;
        }
        if let Some(b) = b_line {
            let _ = writeln!(out, "  + {b}");
            shown += 1;
        }
        if shown >= 60 {
            let _ = writeln!(out, "  ...");
            break;
        }
    }
    out
}
