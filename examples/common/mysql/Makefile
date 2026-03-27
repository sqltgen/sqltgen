.PHONY: db-up db-down

db-up:
	docker compose up -d
	@until bash -c 'echo > /dev/tcp/127.0.0.1/3307' 2>/dev/null; do sleep 0.05; done

db-down:
	docker compose down -v
