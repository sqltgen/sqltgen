include ../../../common.mk

.PHONY: generate test clean db-up db-down

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	go test -count=1 ./...

clean:
	rm -rf gen

db-up:
	docker compose -f $(COMPOSE) up -d postgres
	docker compose -f $(COMPOSE) exec -T postgres sh -c 'until pg_isready -U sqltgen; do sleep 0.2; done'

db-down:
	docker compose -f $(COMPOSE) down postgres
