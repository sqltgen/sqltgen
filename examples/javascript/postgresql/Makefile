SQLTGEN    := ../../../target/debug/sqltgen
PG         := ../../common/postgresql
MIGRATIONS := ../../common/postgresql/migrations
PGHOST     := localhost
PGPORT     := 5433
PGUSER     := sqltgen
PGPASS     := sqltgen

.PHONY: clean generate build db-up db-down run run-shared

node_modules:
	npm install --silent

clean:
	rm -rf gen node_modules

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate node_modules

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

# run-shared: container already running (used by run-all).
run-shared: build
	MIGRATIONS_DIR=$(MIGRATIONS) node main.js

# run: standalone — starts its own container, runs, tears down.
run: build
	$(MAKE) -C $(PG) db-up
	@MIGRATIONS_DIR=$(MIGRATIONS) node main.js; ret=$$?; $(MAKE) -C $(PG) db-down || true; exit $$ret
