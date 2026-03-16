SQLTGEN := ../../../../../../target/debug/sqltgen
COMPOSE := ../../../docker-compose.yml

.PHONY: generate test clean db-up db-down

generate:
	rm -rf src/main/kotlin/com/example/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	mvn -q test

clean:
	mvn -q clean
	rm -rf src/main/kotlin/com/example/db

db-up:
	docker compose -f $(COMPOSE) up -d mysql
	docker compose -f $(COMPOSE) exec -T mysql sh -c 'until mysqladmin ping -u root -psqltgen --silent; do sleep 0.5; done'

db-down:
	docker compose -f $(COMPOSE) down mysql
