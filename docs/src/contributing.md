# Contributing

Thank you for your interest in contributing to sqltgen.

The full contributing guide — build instructions, test suite, code style,
and step-by-step walkthroughs for adding new backends and dialects — is in
[CONTRIBUTING.md](https://github.com/sqltgen/sqltgen/blob/main/CONTRIBUTING.md)
in the repository root.

The detailed technical reference — architecture overview, IR data model, and
guides for adding new backends, dialects, and examples — is in
[docs/contributor-guide.md](https://github.com/sqltgen/sqltgen/blob/main/docs/contributor-guide.md).

## Quick reference

```sh
# Clone and build
git clone https://github.com/sqltgen/sqltgen.git
cd sqltgen
cargo build

# Run all tests
cargo test

# Format and lint
cargo fmt
cargo clippy -- -D warnings
```

## Opening a pull request

1. Fork the repository and create a feature branch.
2. Make your changes; run `cargo fmt` and `cargo clippy`.
3. Add or update tests to cover the change.
4. Open a PR with a clear description of what changes and why.

Keep PRs focused — one logical change per PR makes review much easier.

## Questions?

Open an issue or start a discussion on the
[GitHub repository](https://github.com/sqltgen/sqltgen).
