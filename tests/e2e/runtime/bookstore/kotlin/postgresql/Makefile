SQLTGEN := ../../../../../../target/debug/sqltgen
COMPOSE := ../../../docker-compose.yml

.PHONY: generate test clean db-up db-down

generate:
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	mvn -q test

clean:
	mvn -q clean
	rm -rf src/main/kotlin/com/example/db

db-up:
	docker compose -f $(COMPOSE) up -d postgres
	docker compose -f $(COMPOSE) exec -T postgres sh -c 'until pg_isready -U sqltgen; do sleep 0.2; done'

db-down:
	docker compose -f $(COMPOSE) down postgres
