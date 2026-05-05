SQLTGEN := ./target/debug/sqltgen

.PHONY: all build test generate java kotlin rust python go run-all \
       db-up db-down db-up-mysql db-down-mysql \
       e2e e2e-snapshot e2e-runtime e2e-check-suite \
       e2e-runtime-sqlite e2e-runtime-postgresql e2e-runtime-mysql \
       e2e-db-up e2e-db-down \
       ci-fmt ci-clippy ci-test ci-check-suite ci-examples-drift ci-build \
       ci-runtime-sqlite ci-runtime-postgresql ci-runtime-mysql ci-runtime-db

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
	$(MAKE) -C examples/go      generate

# ── Examples ──────────────────────────────────────────────────────────────────

java: $(SQLTGEN)
	$(MAKE) -C examples/java run

kotlin: $(SQLTGEN)
	$(MAKE) -C examples/kotlin run

rust: $(SQLTGEN)
	$(MAKE) -C examples/rust run

python: $(SQLTGEN)
	$(MAKE) -C examples/python run

go: $(SQLTGEN)
	$(MAKE) -C examples/go run

run-all: $(SQLTGEN)
	# PostgreSQL: one shared container, all four PG examples, then tear down
	$(MAKE) -C examples/common/postgresql    db-up
	$(MAKE) -C examples/java/postgresql      run-shared
	$(MAKE) -C examples/kotlin/postgresql    run-shared
	$(MAKE) -C examples/rust/postgresql      run-shared
	$(MAKE) -C examples/python/postgresql        run-shared
	$(MAKE) -C examples/typescript/postgresql    run-shared
	$(MAKE) -C examples/javascript/postgresql    run-shared
	$(MAKE) -C examples/go/postgresql            run-shared
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
	$(MAKE) -C examples/go/sqlite            run

# ── E2E tests ────────────────────────────────────────────────────────────────

E2E_RUNTIME_DIR := tests/e2e/fixtures

e2e: e2e-snapshot e2e-runtime

# Check that all fixture dialects have the same queries and all runtime suites
# cover every query. Exits 1 if any unexpected gap is found.
e2e-check-suite:
	python tests/e2e/check_suite.py --ci

e2e-snapshot:
	cargo test --test e2e

# Runtime tests: auto-discovered from the filesystem.
# Layout is tests/e2e/fixtures/<fixture>/<engine>/<lang>/sqltgen.json.
# A combo exists iff its directory exists — no exclusion lists, no flags.
#
# Usage:
#   make e2e-runtime                    # all engines, sequential
#   make -j8 --output-sync e2e-runtime  # parallel with clean output
#   make e2e-runtime-sqlite             # only no-Docker engine
#   make e2e-runtime-postgresql         # PG combos (starts Docker)
#   make e2e-runtime-mysql              # MySQL combos (starts Docker)
E2E_SQLITE_COMBOS := $(shell find $(E2E_RUNTIME_DIR) -path '*/sqlite/*/sqltgen.json' -not -path '*/node_modules/*' -not -path '*/target/*' -printf '%h\n' 2>/dev/null | sort)
E2E_PG_COMBOS     := $(shell find $(E2E_RUNTIME_DIR) -path '*/postgresql/*/sqltgen.json' -not -path '*/node_modules/*' -not -path '*/target/*' -printf '%h\n' 2>/dev/null | sort)
E2E_MYSQL_COMBOS  := $(shell find $(E2E_RUNTIME_DIR) -path '*/mysql/*/sqltgen.json' -not -path '*/node_modules/*' -not -path '*/target/*' -printf '%h\n' 2>/dev/null | sort)

# Per-combo targets: tests/e2e/fixtures/<fixture>/<engine>/<lang> → .e2e/<fixture>/<engine>/<lang>
E2E_SQLITE_TARGETS := $(patsubst $(E2E_RUNTIME_DIR)/%,.e2e/%,$(E2E_SQLITE_COMBOS))
E2E_PG_TARGETS     := $(patsubst $(E2E_RUNTIME_DIR)/%,.e2e/%,$(E2E_PG_COMBOS))
E2E_MYSQL_TARGETS  := $(patsubst $(E2E_RUNTIME_DIR)/%,.e2e/%,$(E2E_MYSQL_COMBOS))

e2e-runtime: e2e-runtime-sqlite e2e-runtime-postgresql e2e-runtime-mysql

e2e-runtime-sqlite: $(SQLTGEN) $(E2E_SQLITE_TARGETS)

e2e-runtime-postgresql: $(SQLTGEN) $(E2E_PG_TARGETS)

e2e-runtime-mysql: $(SQLTGEN) $(E2E_MYSQL_TARGETS)

# Ensure Docker is running before any PG/MySQL combo starts.
$(E2E_PG_TARGETS): | e2e-db-up
$(E2E_MYSQL_TARGETS): | e2e-db-up

# Pattern rule: run one combo's tests.
.e2e/%:
	@echo "── $(E2E_RUNTIME_DIR)/$* ──"
	@$(MAKE) -C $(E2E_RUNTIME_DIR)/$* test

# ── E2E Docker lifecycle ────────────────────────────────────────────────────

e2e-db-up:
	docker compose -f tests/e2e/docker-compose.yml up -d --wait --quiet-pull

e2e-db-down:
	docker compose -f tests/e2e/docker-compose.yml down

# ── CI targets ────────────────────────────────────────────────────────────────

ci-build:
	cargo build -q

ci-fmt:
	cargo fmt --check

ci-clippy:
	cargo clippy -- -D warnings

ci-test:
	cargo test

ci-check-suite:
	python tests/e2e/check_suite.py --ci

ci-examples-drift: build
	$(MAKE) generate
	git diff --exit-code -- examples/

ci-runtime-sqlite: ci-build
	pip install --quiet pytest
	$(MAKE) e2e-runtime-sqlite

ci-runtime-postgresql: ci-build
	pip install --quiet pytest "psycopg[binary]"
	$(MAKE) e2e-runtime-postgresql

ci-runtime-mysql: ci-build
	pip install --quiet pytest mysql-connector-python
	$(MAKE) e2e-runtime-mysql

ci-runtime-db: ci-runtime-postgresql ci-runtime-mysql

# ── PostgreSQL database ───────────────────────────────────────────────────────

db-up:
	$(MAKE) -C examples/common/postgresql db-up

db-down:
	$(MAKE) -C examples/common/postgresql db-down

db-up-mysql:
	$(MAKE) -C examples/common/mysql db-up

db-down-mysql:
	$(MAKE) -C examples/common/mysql db-down
