SQLTGEN := ./target/debug/sqltgen

.PHONY: all build test generate java kotlin rust python run-all db-up db-down

all: build test

# ── Core ──────────────────────────────────────────────────────────────────────

build:
	cargo build

test:
	cargo test

# ── Code generation ───────────────────────────────────────────────────────────

# Depends on the binary so it rebuilds sqltgen first if source changed.
$(SQLTGEN): build

generate: $(SQLTGEN)
	$(MAKE) -C examples/java    generate
	$(MAKE) -C examples/kotlin  generate
	$(MAKE) -C examples/rust    generate
	$(MAKE) -C examples/python  generate

# ── Examples ──────────────────────────────────────────────────────────────────

java: $(SQLTGEN)
	$(MAKE) -C examples/java run

kotlin: $(SQLTGEN)
	$(MAKE) -C examples/kotlin run

rust: $(SQLTGEN)
	$(MAKE) -C examples/rust run

python: $(SQLTGEN)
	$(MAKE) -C examples/python run

run-all: java kotlin rust python

# ── PostgreSQL database ───────────────────────────────────────────────────────

db-up:
	$(MAKE) -C examples/common/postgresql db-up

db-down:
	$(MAKE) -C examples/common/postgresql db-down
