use std::collections::{BTreeMap, BTreeSet};

use crate::report::Report;

/// A single ratchet rule violation found when comparing two reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Rule 1: an entity present in both reports got worse individually.
    EntityWorsened { category: String, entity: String, old_excess: u64, new_excess: u64 },
    /// Rule 2: per-category sum of excesses grew between reports.
    CategoryTotalGrew { category: String, old_total: u64, new_total: u64 },
}

/// Apply the ratchet rules to a baseline/current report pair.
///
/// Returns an empty vector when the current report is no worse than the
/// baseline along every category, or a list of violations otherwise.
pub fn check(baseline: &Report, current: &Report) -> Vec<Error> {
    let mut errors = Vec::new();
    let categories: BTreeSet<&String> = baseline.violations.keys().chain(current.violations.keys()).collect();

    let empty: BTreeMap<String, u64> = BTreeMap::new();
    for category in categories {
        let old_v = baseline.violations.get(category).unwrap_or(&empty);
        let new_v = current.violations.get(category).unwrap_or(&empty);

        for (entity, &new_excess) in new_v {
            if let Some(&old_excess) = old_v.get(entity) {
                if new_excess > old_excess {
                    errors.push(Error::EntityWorsened { category: category.clone(), entity: entity.clone(), old_excess, new_excess });
                }
            }
        }

        let old_total: u64 = old_v.values().sum();
        let new_total: u64 = new_v.values().sum();
        if new_total > old_total {
            errors.push(Error::CategoryTotalGrew { category: category.clone(), old_total, new_total });
        }
    }
    errors
}

/// Render a list of ratchet errors as a human-readable string.
pub fn format_errors(errors: &[Error]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for e in errors {
        match e {
            Error::EntityWorsened { category, entity, old_excess, new_excess } => {
                let _ = writeln!(
                    out,
                    "  [{category}] {entity} got worse: {old_excess} → {new_excess} \
                     (excess over threshold)"
                );
            },
            Error::CategoryTotalGrew { category, old_total, new_total } => {
                let _ = writeln!(
                    out,
                    "  [{category}] total excess grew: {old_total} → {new_total} \
                     (improve elsewhere or revert)"
                );
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::default_thresholds;

    fn report(violations: Vec<(&str, Vec<(&str, u64)>)>) -> Report {
        let mut map: crate::report::CategoryMap = BTreeMap::new();
        for (cat, entries) in violations {
            let mut m = BTreeMap::new();
            for (k, v) in entries {
                m.insert(k.to_string(), v);
            }
            map.insert(cat.to_string(), m);
        }
        // Pre-populate any missing default categories with empty maps.
        for k in default_thresholds().keys() {
            map.entry(k.clone()).or_default();
        }
        Report::new(default_thresholds(), map)
    }

    #[test]
    fn test_check_passes_when_reports_are_identical() {
        let baseline = report(vec![("function_lines", vec![("a.rs::foo", 10)])]);
        let current = baseline.clone();
        assert!(check(&baseline, &current).is_empty());
    }

    #[test]
    fn test_check_fails_when_existing_entity_grows() {
        let baseline = report(vec![("function_lines", vec![("a.rs::foo", 10)])]);
        let current = report(vec![("function_lines", vec![("a.rs::foo", 11)])]);
        let errors = check(&baseline, &current);
        assert_eq!(errors.len(), 2); // entity worsened + category total grew
        assert!(matches!(errors[0], Error::EntityWorsened { .. }));
    }

    #[test]
    fn test_check_passes_when_existing_entity_improves() {
        let baseline = report(vec![("function_lines", vec![("a.rs::foo", 10)])]);
        let current = report(vec![("function_lines", vec![("a.rs::foo", 5)])]);
        assert!(check(&baseline, &current).is_empty());
    }

    #[test]
    fn test_check_passes_when_split_reduces_total_violation() {
        // fileA had excess 800 → split into fileB (300) + fileC (300) = 600 total.
        let baseline = report(vec![("file_lines", vec![("a.rs", 800)])]);
        let current = report(vec![("file_lines", vec![("b.rs", 300), ("c.rs", 300)])]);
        assert!(check(&baseline, &current).is_empty());
    }

    #[test]
    fn test_check_fails_when_new_entity_pushes_total_up() {
        let baseline = report(vec![("file_lines", vec![("a.rs", 100)])]);
        let current = report(vec![("file_lines", vec![("a.rs", 100), ("b.rs", 50)])]);
        let errors = check(&baseline, &current);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], Error::CategoryTotalGrew { .. }));
    }

    #[test]
    fn test_check_passes_when_new_entity_absorbed_by_improvement() {
        // a.rs improves 100 → 0; b.rs new at 50; total 100 → 50, OK.
        let baseline = report(vec![("file_lines", vec![("a.rs", 100)])]);
        let current = report(vec![("file_lines", vec![("b.rs", 50)])]);
        assert!(check(&baseline, &current).is_empty());
    }

    #[test]
    fn test_check_treats_rename_as_remove_plus_add() {
        // foo renamed to bar at same size → totals unchanged → OK.
        let baseline = report(vec![("function_lines", vec![("a.rs::foo", 12)])]);
        let current = report(vec![("function_lines", vec![("a.rs::bar", 12)])]);
        assert!(check(&baseline, &current).is_empty());
    }

    #[test]
    fn test_check_blocks_rename_with_growth() {
        let baseline = report(vec![("function_lines", vec![("a.rs::foo", 12)])]);
        let current = report(vec![("function_lines", vec![("a.rs::bar", 15)])]);
        let errors = check(&baseline, &current);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], Error::CategoryTotalGrew { .. }));
    }

    #[test]
    fn test_check_categories_ratchet_independently() {
        // function_lines improves by 10; file_lines worsens by 5.
        // file_lines must fail in isolation; cross-category compensation is not allowed.
        let baseline = report(vec![("function_lines", vec![("a.rs::foo", 10)]), ("file_lines", vec![("a.rs", 100)])]);
        let current = report(vec![("function_lines", vec![("a.rs::foo", 0)]), ("file_lines", vec![("a.rs", 105)])]);
        let errors = check(&baseline, &current);
        assert!(errors.iter().any(|e| matches!(
            e,
            Error::EntityWorsened { category, .. } if category == "file_lines"
        )));
        assert!(errors.iter().any(|e| matches!(
            e,
            Error::CategoryTotalGrew { category, .. } if category == "file_lines"
        )));
    }

    #[test]
    fn test_check_passes_when_deletion_frees_budget_for_new_violation() {
        let baseline = report(vec![("file_lines", vec![("a.rs", 200), ("b.rs", 50)])]);
        let current = report(vec![("file_lines", vec![("c.rs", 150)])]);
        // Removed: 200 + 50 = 250. Added: 150. Net 250 → 150, OK.
        assert!(check(&baseline, &current).is_empty());
    }
}
