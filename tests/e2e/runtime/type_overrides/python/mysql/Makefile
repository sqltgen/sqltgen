SQLTGEN  := ../../../../../../target/debug/sqltgen
COMPOSE  := ../../../../docker-compose.yml

.PHONY: generate test clean db-up db-down

generate:
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	python3 -m pytest test_runtime.py -v

clean:
	rm -rf gen __pycache__ .pytest_cache

db-up:
	docker compose -f $(COMPOSE) up -d mysql
	docker compose -f $(COMPOSE) exec -T mysql sh -c 'until mysqladmin ping -u root -psqltgen --silent; do sleep 0.5; done'

db-down:
	docker compose -f $(COMPOSE) down mysql
