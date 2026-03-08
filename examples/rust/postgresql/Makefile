SQLTGEN    := ../../../target/debug/sqltgen
PG         := ../../common/postgresql
MIGRATIONS := ../../common/postgresql/migrations
PGHOST     := localhost
PGPORT     := 5433
PGUSER     := sqltgen
PGPASS     := sqltgen

.PHONY: clean generate build db-up db-down run run-shared

clean:
	cargo clean
	rm -rf src/db

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

build:
	cargo build

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

# run-shared: container already running (used by run-all).
run-shared: generate
	MIGRATIONS_DIR=$(MIGRATIONS) cargo run

# run: standalone — starts its own container, runs, tears down.
run: generate
	$(MAKE) -C $(PG) db-up
	@MIGRATIONS_DIR=$(MIGRATIONS) cargo run; ret=$$?; $(MAKE) -C $(PG) db-down || true; exit $$ret
