include ../../../common.mk

.PHONY: generate test clean db-up db-down

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	cargo test

clean:
	cargo clean
	rm -rf src/db

db-up:
	docker compose -f $(COMPOSE) up -d mysql
	docker compose -f $(COMPOSE) exec -T mysql sh -c 'until mysqladmin ping -h localhost --silent; do sleep 0.2; done'

db-down:
	docker compose -f $(COMPOSE) down mysql
