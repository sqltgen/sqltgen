SQLTGEN    := ../../../target/debug/sqltgen
MYSQL      := ../../common/mysql
MIGRATIONS := ../../common/mysql/migrations

.PHONY: clean generate build db-up db-down run run-shared

clean:
	rm -rf gen

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

# run-shared: container already running (used by run-all).
run-shared: generate
	docker compose up --build --abort-on-container-exit demo
	@docker compose rm -f demo >/dev/null 2>&1 || true

# run: standalone — starts its own container, runs, tears down.
run: generate
	docker compose up --build --abort-on-container-exit demo
	@ret=$$?; docker compose down; exit $$ret
