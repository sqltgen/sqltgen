SQLTGEN    := ../../../target/debug/sqltgen
PG         := ../../common/postgresql
MIGRATIONS := ../../common/postgresql/migrations

.PHONY: clean generate build db-up db-down run run-shared

clean:
	rm -rf gen

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

# run-shared: container already running (used by run-all).
run-shared: build
	MIGRATIONS_DIR=$(MIGRATIONS) go run main.go

# run: standalone — starts its own container, runs, tears down.
run: build
	$(MAKE) -C $(PG) db-up
	@MIGRATIONS_DIR=$(MIGRATIONS) go run main.go; ret=$$?; $(MAKE) -C $(PG) db-down || true; exit $$ret
