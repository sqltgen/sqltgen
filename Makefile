SQLTGEN := ./target/debug/sqltgen

.PHONY: all build test generate java kotlin rust python go run-all \
       db-up db-down db-up-mysql db-down-mysql \
       e2e e2e-snapshot e2e-runtime e2e-check-suite \
       e2e-runtime-rust-sqlite e2e-runtime-rust-postgresql e2e-runtime-rust-mysql \
       e2e-runtime-java-postgresql e2e-runtime-java-sqlite e2e-runtime-java-mysql \
       e2e-runtime-kotlin-postgresql e2e-runtime-kotlin-sqlite e2e-runtime-kotlin-mysql \
       e2e-runtime-python-sqlite e2e-runtime-python-postgresql e2e-runtime-python-mysql \
       e2e-runtime-typescript-sqlite e2e-runtime-typescript-postgresql e2e-runtime-typescript-mysql \
       e2e-runtime-go-sqlite e2e-runtime-go-postgresql e2e-runtime-go-mysql \
       e2e-runtime-type-overrides \
       e2e-runtime-type-overrides-rust-sqlite \
       e2e-runtime-type-overrides-rust-postgresql \
       e2e-runtime-type-overrides-rust-mysql \
       e2e-runtime-type-overrides-java-postgresql \
       e2e-runtime-type-overrides-java-postgresql-gson \
       e2e-runtime-type-overrides-java-sqlite \
       e2e-runtime-type-overrides-java-mysql \
       e2e-runtime-type-overrides-kotlin-postgresql \
       e2e-runtime-type-overrides-kotlin-postgresql-gson \
       e2e-runtime-type-overrides-kotlin-sqlite \
       e2e-runtime-type-overrides-kotlin-mysql \
       e2e-runtime-type-overrides-python-sqlite \
       e2e-runtime-type-overrides-python-postgresql \
       e2e-runtime-type-overrides-python-mysql \
       e2e-runtime-type-overrides-typescript-sqlite \
       e2e-runtime-type-overrides-typescript-postgresql \
       e2e-runtime-type-overrides-typescript-mysql \
       e2e-runtime-type-overrides-go-sqlite \
       e2e-runtime-type-overrides-go-postgresql \
       e2e-runtime-type-overrides-go-mysql \
       e2e-runtime-enums \
       e2e-runtime-enums-java-postgresql \
       e2e-runtime-enums-kotlin-postgresql \
       e2e-runtime-enums-python-postgresql \
       e2e-runtime-enums-typescript-postgresql \
       e2e-runtime-enums-go-postgresql \
       e2e-runtime-enums-rust-postgresql \
       e2e-db-up e2e-db-down \
       e2e-testgen-setup e2e-testgen-generate e2e-testgen-generate-python \
       e2e-new-scaffold e2e-new-test e2e-new-test-sqlite e2e-new-test-postgresql e2e-new-test-mysql \
       ci-fmt ci-clippy ci-test ci-check-suite ci-examples-drift ci-testgen-mypy ci-testgen-drift ci-build ci-runtime-sqlite ci-runtime-postgresql ci-runtime-mysql ci-runtime-db

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

E2E_RUNTIME        := tests/e2e/runtime/bookstore
E2E_RUNTIME_DIR    := tests/e2e/runtime
E2E_TYPE_OVERRIDES := tests/e2e/runtime/type_overrides
E2E_TESTGEN        := scripts/e2e_testgen
E2E_TESTGEN_PYTHON := $(E2E_TESTGEN)/.venv/bin/python
E2E_TESTGEN_STAMP  := $(E2E_TESTGEN)/.venv/.stamp

e2e: e2e-snapshot e2e-runtime

# Check that all fixture dialects have the same queries and all runtime suites
# cover every query. Exits 1 if any unexpected gap is found.
e2e-check-suite:
	python tests/e2e/check_suite.py --ci

e2e-snapshot:
	cargo test --test e2e

# Runtime tests: regenerate code, then run each sub-project's tests.
# SQLite tests need no Docker; PG/MySQL targets will start containers.

e2e-runtime: \
	e2e-runtime-python-sqlite \
	e2e-runtime-typescript-sqlite \
	e2e-runtime-rust-sqlite \
	e2e-runtime-java-sqlite \
	e2e-runtime-kotlin-sqlite \
	e2e-runtime-go-sqlite \
	e2e-runtime-rust-postgresql \
	e2e-runtime-rust-mysql \
	e2e-runtime-java-postgresql \
	e2e-runtime-java-mysql \
	e2e-runtime-kotlin-postgresql \
	e2e-runtime-kotlin-mysql \
	e2e-runtime-python-postgresql \
	e2e-runtime-python-mysql \
	e2e-runtime-typescript-postgresql \
	e2e-runtime-typescript-mysql \
	e2e-runtime-go-postgresql \
	e2e-runtime-go-mysql \
	e2e-runtime-type-overrides \
	e2e-runtime-enums

# ── Type-overrides runtime tests (all dialects × all languages) ───────────────

# SQLite-only (no Docker needed)
e2e-runtime-type-overrides-rust-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/rust/sqlite test

e2e-runtime-type-overrides-python-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/python/sqlite test

e2e-runtime-type-overrides-typescript-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/typescript/sqlite install test

e2e-runtime-type-overrides-java-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/java/sqlite test

e2e-runtime-type-overrides-kotlin-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/kotlin/sqlite test

e2e-runtime-type-overrides-go-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/go/sqlite test

# Docker-based (PostgreSQL + MySQL)
e2e-runtime-type-overrides-rust-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/rust/postgresql test

e2e-runtime-type-overrides-rust-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/rust/mysql test

e2e-runtime-type-overrides-java-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/java/postgresql test

e2e-runtime-type-overrides-java-postgresql-gson: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/java/postgresql-gson test

e2e-runtime-type-overrides-java-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/java/mysql test

e2e-runtime-type-overrides-kotlin-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/kotlin/postgresql test

e2e-runtime-type-overrides-kotlin-postgresql-gson: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/kotlin/postgresql-gson test

e2e-runtime-type-overrides-kotlin-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/kotlin/mysql test

e2e-runtime-type-overrides-python-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/python/postgresql test

e2e-runtime-type-overrides-python-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/python/mysql test

e2e-runtime-type-overrides-typescript-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/typescript/postgresql install test

e2e-runtime-type-overrides-typescript-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/typescript/mysql install test

e2e-runtime-type-overrides-go-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/go/postgresql test

e2e-runtime-type-overrides-go-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_TYPE_OVERRIDES)/go/mysql test

# Aggregator: SQLite first (no Docker), then Docker-based
e2e-runtime-type-overrides: \
	e2e-runtime-type-overrides-rust-sqlite \
	e2e-runtime-type-overrides-python-sqlite \
	e2e-runtime-type-overrides-typescript-sqlite \
	e2e-runtime-type-overrides-java-sqlite \
	e2e-runtime-type-overrides-kotlin-sqlite \
	e2e-runtime-type-overrides-go-sqlite \
	e2e-runtime-type-overrides-rust-postgresql \
	e2e-runtime-type-overrides-rust-mysql \
	e2e-runtime-type-overrides-java-postgresql \
	e2e-runtime-type-overrides-java-postgresql-gson \
	e2e-runtime-type-overrides-java-mysql \
	e2e-runtime-type-overrides-kotlin-postgresql \
	e2e-runtime-type-overrides-kotlin-postgresql-gson \
	e2e-runtime-type-overrides-kotlin-mysql \
	e2e-runtime-type-overrides-python-postgresql \
	e2e-runtime-type-overrides-python-mysql \
	e2e-runtime-type-overrides-typescript-postgresql \
	e2e-runtime-type-overrides-typescript-mysql \
	e2e-runtime-type-overrides-go-postgresql \
	e2e-runtime-type-overrides-go-mysql

# ── Enums runtime tests (PostgreSQL only) ─────────────────────────────────────

E2E_ENUMS := tests/e2e/runtime/enums

e2e-runtime-enums-java-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_ENUMS)/java/postgresql test

e2e-runtime-enums-kotlin-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_ENUMS)/kotlin/postgresql test

e2e-runtime-enums-python-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_ENUMS)/python/postgresql test

e2e-runtime-enums-typescript-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_ENUMS)/typescript/postgresql install test

e2e-runtime-enums-go-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_ENUMS)/go/postgresql test

e2e-runtime-enums-rust-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_ENUMS)/rust/postgresql test

e2e-runtime-enums: \
	e2e-runtime-enums-java-postgresql \
	e2e-runtime-enums-kotlin-postgresql \
	e2e-runtime-enums-python-postgresql \
	e2e-runtime-enums-typescript-postgresql \
	e2e-runtime-enums-go-postgresql \
	e2e-runtime-enums-rust-postgresql

# ── No-Docker runtime tests (SQLite) ─────────────────────────────────────────

e2e-runtime-rust-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_RUNTIME)/rust/sqlite test

e2e-runtime-python-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_RUNTIME)/python/sqlite test

e2e-runtime-typescript-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_RUNTIME)/typescript/sqlite install test

e2e-runtime-java-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_RUNTIME)/java/sqlite test

e2e-runtime-kotlin-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_RUNTIME)/kotlin/sqlite test

e2e-runtime-go-sqlite: $(SQLTGEN)
	$(MAKE) -C $(E2E_RUNTIME)/go/sqlite test

# ── Docker-based runtime tests (PostgreSQL + MySQL) ───────────────────────────

e2e-runtime-rust-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/rust/postgresql test

e2e-runtime-rust-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/rust/mysql test

e2e-runtime-java-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/java/postgresql test

e2e-runtime-java-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/java/mysql test

e2e-runtime-kotlin-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/kotlin/postgresql test

e2e-runtime-kotlin-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/kotlin/mysql test

e2e-runtime-python-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/python/postgresql test

e2e-runtime-python-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/python/mysql test

e2e-runtime-typescript-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/typescript/postgresql install test

e2e-runtime-typescript-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/typescript/mysql install test

e2e-runtime-go-postgresql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/go/postgresql test

e2e-runtime-go-mysql: $(SQLTGEN) e2e-db-up
	$(MAKE) -C $(E2E_RUNTIME)/go/mysql test

# ── E2E Docker lifecycle ────────────────────────────────────────────────────

e2e-db-up:
	docker compose -f $(E2E_RUNTIME_DIR)/docker-compose.yml up -d --wait --quiet-pull

e2e-db-down:
	docker compose -f $(E2E_RUNTIME_DIR)/docker-compose.yml down

# ── E2E test generation (dynamic test files from test_spec.yaml) ──────────────

# Stamp target: create the venv and install deps when requirements change.
$(E2E_TESTGEN_STAMP): $(E2E_TESTGEN)/requirements.txt
	python3 -m venv $(E2E_TESTGEN)/.venv
	$(E2E_TESTGEN_PYTHON) -m pip install -q -r $(E2E_TESTGEN)/requirements.txt
	touch $(E2E_TESTGEN_STAMP)

e2e-testgen-setup: $(E2E_TESTGEN_STAMP)

# Generate test files for every fixture × language × engine × variant combo that
# has a test_spec.yaml and a sqltgen.json. Accepts optional overrides:
#   make e2e-testgen-generate TESTGEN_LANG=python TESTGEN_ENGINE=postgresql TESTGEN_VARIANT=gson
e2e-testgen-generate: $(E2E_TESTGEN_STAMP) $(SQLTGEN)
	$(E2E_TESTGEN_PYTHON) $(E2E_TESTGEN)/orchestrate.py generate \
	    $(if $(TESTGEN_FIXTURE),--fixture $(TESTGEN_FIXTURE)) \
	    $(if $(TESTGEN_LANG),--lang $(TESTGEN_LANG)) \
	    $(if $(TESTGEN_ENGINE),--engine $(TESTGEN_ENGINE)) \
	    $(if $(TESTGEN_VARIANT),--variant $(TESTGEN_VARIANT)) \
	    --sqltgen $(abspath $(SQLTGEN))

# Convenience shorthand: generate Python test files only.
e2e-testgen-generate-python: $(E2E_TESTGEN_STAMP) $(SQLTGEN)
	$(E2E_TESTGEN_PYTHON) $(E2E_TESTGEN)/orchestrate.py generate \
	    --lang python \
	    --sqltgen $(abspath $(SQLTGEN))

# ── E2E runtime-new (auto-discovered, fully generated) ───────────────────────

E2E_NEW_DIR := tests/e2e/runtime-new

# Scaffold all projects + generate test files from test_spec.yaml.
e2e-new-scaffold: $(E2E_TESTGEN_STAMP) $(SQLTGEN)
	$(E2E_TESTGEN_PYTHON) $(E2E_TESTGEN)/scaffold.py --all
	$(E2E_TESTGEN_PYTHON) $(E2E_TESTGEN)/orchestrate.py generate \
	    --runtime-dir $(E2E_NEW_DIR) \
	    --sqltgen $(abspath $(SQLTGEN))

# Run all runtime-new tests. Each sub-Makefile invokes sqltgen and runs tests.
E2E_NEW_COMBOS := $(shell find $(E2E_NEW_DIR) -name sqltgen.json -not -path '*/node_modules/*' -printf '%h\n' 2>/dev/null | sort)
E2E_NEW_SQLITE := $(filter %/sqlite, $(E2E_NEW_COMBOS))
E2E_NEW_PG     := $(filter %/postgresql, $(E2E_NEW_COMBOS))
E2E_NEW_MYSQL  := $(filter %/mysql, $(E2E_NEW_COMBOS))

e2e-new-test: e2e-new-test-sqlite e2e-new-test-postgresql e2e-new-test-mysql

e2e-new-test-sqlite: $(SQLTGEN)
	@for d in $(E2E_NEW_SQLITE); do echo "── $$d ──" && $(MAKE) -C $$d test || exit 1; done

e2e-new-test-postgresql: $(SQLTGEN) e2e-db-up
	@for d in $(E2E_NEW_PG); do echo "── $$d ──" && $(MAKE) -C $$d test || exit 1; done

e2e-new-test-mysql: $(SQLTGEN) e2e-db-up
	@for d in $(E2E_NEW_MYSQL); do echo "── $$d ──" && $(MAKE) -C $$d test || exit 1; done

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

ci-testgen-mypy: $(E2E_TESTGEN_STAMP)
	cd $(E2E_TESTGEN) && .venv/bin/python -m mypy --explicit-package-bases codegen.py orchestrate.py manifest.py test_spec.py literals/

ci-testgen-drift: build
	$(MAKE) e2e-testgen-setup
	$(MAKE) e2e-testgen-generate
	git diff --exit-code -- tests/e2e/runtime/

ci-runtime-sqlite: ci-build
	pip install --quiet pytest
	$(MAKE) e2e-runtime-rust-sqlite
	$(MAKE) e2e-runtime-python-sqlite
	$(MAKE) e2e-runtime-typescript-sqlite
	$(MAKE) e2e-runtime-java-sqlite
	$(MAKE) e2e-runtime-kotlin-sqlite
	$(MAKE) e2e-runtime-go-sqlite
	$(MAKE) e2e-runtime-type-overrides-rust-sqlite
	$(MAKE) e2e-runtime-type-overrides-python-sqlite
	$(MAKE) e2e-runtime-type-overrides-typescript-sqlite
	$(MAKE) e2e-runtime-type-overrides-java-sqlite
	$(MAKE) e2e-runtime-type-overrides-kotlin-sqlite
	$(MAKE) e2e-runtime-type-overrides-go-sqlite

ci-runtime-postgresql: ci-build
	pip install --quiet pytest "psycopg[binary]"
	$(MAKE) e2e-runtime-rust-postgresql
	$(MAKE) e2e-runtime-java-postgresql
	$(MAKE) e2e-runtime-kotlin-postgresql
	$(MAKE) e2e-runtime-python-postgresql
	$(MAKE) e2e-runtime-typescript-postgresql
	$(MAKE) e2e-runtime-go-postgresql
	$(MAKE) e2e-runtime-type-overrides-rust-postgresql
	$(MAKE) e2e-runtime-type-overrides-java-postgresql
	$(MAKE) e2e-runtime-type-overrides-java-postgresql-gson
	$(MAKE) e2e-runtime-type-overrides-kotlin-postgresql
	$(MAKE) e2e-runtime-type-overrides-kotlin-postgresql-gson
	$(MAKE) e2e-runtime-type-overrides-python-postgresql
	$(MAKE) e2e-runtime-type-overrides-typescript-postgresql
	$(MAKE) e2e-runtime-type-overrides-go-postgresql
	$(MAKE) e2e-runtime-enums

ci-runtime-mysql: ci-build
	pip install --quiet pytest mysql-connector-python
	$(MAKE) e2e-runtime-rust-mysql
	$(MAKE) e2e-runtime-java-mysql
	$(MAKE) e2e-runtime-kotlin-mysql
	$(MAKE) e2e-runtime-python-mysql
	$(MAKE) e2e-runtime-typescript-mysql
	$(MAKE) e2e-runtime-go-mysql
	$(MAKE) e2e-runtime-type-overrides-rust-mysql
	$(MAKE) e2e-runtime-type-overrides-java-mysql
	$(MAKE) e2e-runtime-type-overrides-kotlin-mysql
	$(MAKE) e2e-runtime-type-overrides-python-mysql
	$(MAKE) e2e-runtime-type-overrides-typescript-mysql
	$(MAKE) e2e-runtime-type-overrides-go-mysql

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
