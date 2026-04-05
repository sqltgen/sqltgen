include ../../../common.mk

.PHONY: generate test clean install db-up db-down

install:
	npm install

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	npm test

clean:
	rm -rf gen node_modules

db-up:
	docker compose -f $(COMPOSE) up -d postgres
	docker compose -f $(COMPOSE) exec -T postgres sh -c 'until pg_isready -U sqltgen; do sleep 0.2; done'

db-down:
	docker compose -f $(COMPOSE) down postgres
