.PHONY: db-up db-down

db-up:
	docker compose up -d
	@until pg_isready -h localhost -p 5433 -U sqltgen -q; do sleep 0.05; done

db-down:
	docker compose down -v
