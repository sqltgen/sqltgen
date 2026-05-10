use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::collectors::Collector;

pub const SCHEMA_VERSION: u32 = 1;

pub type CategoryMap = BTreeMap<String, BTreeMap<String, u64>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    pub schema_version: u32,
    pub thresholds: BTreeMap<String, u64>,
    pub totals: BTreeMap<String, u64>,
    pub violations: CategoryMap,
}

impl Report {
    pub fn new(thresholds: BTreeMap<String, u64>, violations: CategoryMap) -> Self {
        let totals = compute_totals(&violations);
        Self { schema_version: SCHEMA_VERSION, thresholds, totals, violations }
    }

    pub fn read_from(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_str(&content).with_context(|| format!("parsing {}", path.display()))
    }

    pub fn write_to(&self, path: &Path) -> Result<()> {
        let mut content = self.to_pretty_string();
        content.push('\n');
        fs::write(path, content).with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }

    pub fn to_pretty_string(&self) -> String {
        serde_json::to_string_pretty(self).expect("Report serialization is infallible")
    }
}

fn compute_totals(violations: &CategoryMap) -> BTreeMap<String, u64> {
    violations.iter().map(|(cat, entries)| (cat.clone(), entries.values().sum())).collect()
}

/// Default thresholds for the v1 metric set (CLAUDE.md "yellow-flag" levels).
pub fn default_thresholds() -> BTreeMap<String, u64> {
    let mut t = BTreeMap::new();
    t.insert("file_functions".into(), 20);
    t.insert("file_lines".into(), 300);
    t.insert("function_args".into(), 4);
    t.insert("function_cognitive".into(), 10);
    t.insert("function_cyclomatic".into(), 10);
    t.insert("function_lines".into(), 50);
    t.insert("module_files".into(), 20);
    t
}

/// Run all collectors and produce a report.
pub fn generate(workspace_root: &Path) -> Result<Report> {
    let thresholds = default_thresholds();
    let collectors: Vec<Box<dyn Collector>> = vec![Box::new(crate::collectors::structural::Structural::new(thresholds.clone()))];

    let mut violations: CategoryMap = thresholds.keys().map(|k| (k.clone(), BTreeMap::new())).collect();

    for collector in &collectors {
        for (category, entries) in collector.collect(workspace_root)? {
            violations.entry(category).or_default().extend(entries);
        }
    }

    Ok(Report::new(thresholds, violations))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_totals_sums_violations_per_category() {
        let mut violations: CategoryMap = BTreeMap::new();
        let mut fn_lines = BTreeMap::new();
        fn_lines.insert("a.rs::foo".into(), 5);
        fn_lines.insert("a.rs::bar".into(), 3);
        violations.insert("function_lines".into(), fn_lines);
        violations.insert("file_lines".into(), BTreeMap::new());

        let totals = compute_totals(&violations);
        assert_eq!(totals.get("function_lines"), Some(&8));
        assert_eq!(totals.get("file_lines"), Some(&0));
    }

    #[test]
    fn test_report_round_trip_through_json() {
        let mut violations = BTreeMap::new();
        let mut entries = BTreeMap::new();
        entries.insert("src/foo.rs::bar".into(), 12);
        violations.insert("function_lines".into(), entries);
        violations.insert("file_lines".into(), BTreeMap::new());

        let original = Report::new(default_thresholds(), violations);
        let json = original.to_pretty_string();
        let parsed: Report = serde_json::from_str(&json).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_report_pretty_string_is_deterministic() {
        let mut violations = BTreeMap::new();
        // Insert in non-sorted order to verify BTreeMap sorts on serialize
        let mut entries = BTreeMap::new();
        entries.insert("z::z".into(), 1);
        entries.insert("a::a".into(), 2);
        violations.insert("function_lines".into(), entries);

        let r1 = Report::new(default_thresholds(), violations.clone()).to_pretty_string();
        let r2 = Report::new(default_thresholds(), violations).to_pretty_string();
        assert_eq!(r1, r2);
        // Sanity: keys are sorted
        let a_pos = r1.find("a::a").unwrap();
        let z_pos = r1.find("z::z").unwrap();
        assert!(a_pos < z_pos, "keys should be sorted alphabetically");
    }
}
