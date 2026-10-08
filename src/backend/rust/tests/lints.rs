//! Generated Rust must pass `cargo clippy -- -D warnings` in a downstream project.
//! sqltgen handles this inside the generated code via crate-wide allows on the
//! generated root module (they propagate to the submodules). Formatting is left to
//! the consumer, who exempts the module with `#[rustfmt::skip]` (see the README).

use super::*;

#[test]
fn test_generate_root_mod_emits_clippy_allows() {
    let schema = Schema::with_tables(vec![user_table()]);
    let query = Query::exec("DeleteUser", "DELETE FROM user WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)]);
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let root = get_file_by_path(&files, "mod.rs");
    // `too_many_arguments` covers wide querier methods; `module_inception` covers a
    // query group named after its module (`queries::queries`); `dead_code` covers
    // querier functions the consumer does not call.
    assert!(root.contains("#![allow(dead_code)]"));
    assert!(root.contains("#![allow(clippy::too_many_arguments)]"));
    assert!(root.contains("#![allow(clippy::module_inception)]"));
}
