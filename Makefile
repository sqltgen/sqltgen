SQLTGEN := ./target/debug/sqltgen

.PHONY: all build test generate java kotlin rust python run-all \
       db-up db-down db-up-mysql db-down-mysql \
       e2e e2e-snapshot e2e-runtime \
       e2e-runtime-rust-sqlite e2e-runtime-rust-postgresql \
       e2e-db-up e2e-db-down

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

run-all: $(SQLTGEN)
	# PostgreSQL: one shared container, all four PG examples, then tear down
	$(MAKE) -C examples/common/postgresql    db-up
	$(MAKE) -C examples/java/postgresql      run-shared
	$(MAKE) -C examples/kotlin/postgresql    run-shared
	$(MAKE) -C examples/rust/postgresql      run-shared
	$(MAKE) -C examples/python/postgresql        run-shared
	$(MAKE) -C examples/typescript/postgresql    run-shared
	$(MAKE) -C examples/javascript/postgresql    run-shared
	$(MAKE) -C examples/common/postgresql        db-down
	# MySQL: one shared container, all four MySQL examples, then tear down
	$(MAKE) -C examples/common/mysql         db-up
	$(MAKE) -C examples/java/mysql           run-shared
	$(MAKE) -C examples/kotlin/mysql         run-shared
	$(MAKE) -C examples/rust/mysql           run-shared
	$(MAKE) -C examples/python/mysql         run-shared
	$(MAKE) -C examples/typescript/mysql     run-shared
	$(MAKE) -C examples/javascript/mysql     run-shared
	$(MAKE) -C examples/common/mysql         db-down
	# SQLite: no containers
	$(MAKE) -C examples/java/sqlite          run
	$(MAKE) -C examples/kotlin/sqlite        run
	$(MAKE) -C examples/rust/sqlite          run
	$(MAKE) -C examples/python/sqlite        run
	$(MAKE) -C examples/typescript/sqlite    run
	$(MAKE) -C examples/javascript/sqlite    run

# ── E2E tests ────────────────────────────────────────────────────────────────

E2E_RUNTIME := tests/e2e/runtime

e2e: e2e-snapshot e2e-runtime

e2e-snapshot:
	cargo test --test e2e

# Runtime tests: regenerate code, then run each sub-project's tests.
# SQLite tests need no Docker; PG/MySQL targets will start containers.

e2e-runtime: e2e-runtime-rust-sqlite e2e-runtime-rust-postgresql

e2e-runtime-rust-sqlite: $(SQLTGEN)
	cd $(E2E_RUNTIME)/rust/sqlite && $(abspath $(SQLTGEN)) generate --config sqltgen.json
	cd $(E2E_RUNTIME)/rust/sqlite && cargo test

e2e-runtime-rust-postgresql: $(SQLTGEN) e2e-db-up
	cd $(E2E_RUNTIME)/rust/postgresql && $(abspath $(SQLTGEN)) generate --config sqltgen.json
	cd $(E2E_RUNTIME)/rust/postgresql && cargo test

# ── E2E Docker lifecycle ────────────────────────────────────────────────────

e2e-db-up:
	docker compose -f $(E2E_RUNTIME)/docker-compose.yml up -d --wait

e2e-db-down:
	docker compose -f $(E2E_RUNTIME)/docker-compose.yml down

# ── PostgreSQL database ───────────────────────────────────────────────────────

db-up:
	$(MAKE) -C examples/common/postgresql db-up

db-down:
	$(MAKE) -C examples/common/postgresql db-down

db-up-mysql:
	$(MAKE) -C examples/common/mysql db-up

db-down-mysql:
	$(MAKE) -C examples/common/mysql db-down
