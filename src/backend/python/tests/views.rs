use super::*;

#[test]
fn test_generate_view_dataclass() {
    let schema = Schema::with_tables(vec![user_summary_view()]);
    let files = pg().generate(&schema, &[], &cfg()).unwrap();
    let src = get_file(&files, "user_summary.py");
    assert!(src.contains("class UserSummary:"));
    assert!(src.contains("id: int"));
    assert!(src.contains("display_name: str"));
}
