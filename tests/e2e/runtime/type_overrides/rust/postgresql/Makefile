include ../../../common.mk

.PHONY: generate test clean db-up db-down

generate:
	rm -rf src/db src_chrono/db
	$(SQLTGEN) generate --config sqltgen.json
	$(SQLTGEN) generate --config sqltgen-chrono.json

test: generate
	cargo test -q --test runtime
	cargo test -q --test chrono_runtime

clean:
	cargo clean
	rm -rf src/db src_chrono/db

db-up:
	docker compose -f $(COMPOSE) up -d postgres
	docker compose -f $(COMPOSE) exec -T postgres sh -c 'until pg_isready -U sqltgen; do sleep 0.2; done'

db-down:
	docker compose -f $(COMPOSE) down postgres
