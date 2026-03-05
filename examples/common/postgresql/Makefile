.PHONY: db-up db-down

db-up:
	docker compose up -d --wait
	@until pg_isready -h localhost -p 5433 -U sqltgen -q; do sleep 0.2; done

db-down:
	docker compose down
