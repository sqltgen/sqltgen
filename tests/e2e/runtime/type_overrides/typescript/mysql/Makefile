include ../../../common.mk

.PHONY: generate test clean install db-up db-down

install:
	npm install --silent

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	npm test

clean:
	rm -rf gen node_modules

db-up:
	docker compose -f $(COMPOSE) up -d mysql
	docker compose -f $(COMPOSE) exec -T mysql sh -c 'until mysqladmin ping -u root -psqltgen --silent; do sleep 0.5; done'

db-down:
	docker compose -f $(COMPOSE) down mysql
