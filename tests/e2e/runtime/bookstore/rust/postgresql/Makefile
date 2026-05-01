include ../../../common.mk

.PHONY: generate test clean db-up db-down

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	cargo test -q

clean:
	cargo clean
	rm -rf src/db

db-up:
	docker compose -f $(COMPOSE) up -d postgres
	docker compose -f $(COMPOSE) exec -T postgres sh -c 'until pg_isready -U sqltgen; do sleep 0.2; done'

db-down:
	docker compose -f $(COMPOSE) down postgres
