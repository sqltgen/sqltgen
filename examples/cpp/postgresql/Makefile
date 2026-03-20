SQLTGEN    := ../../../target/debug/sqltgen

.PHONY: clean generate run

clean:
	rm -rf gen

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

run: generate
	docker compose up --build --abort-on-container-exit demo
	@ret=$$?; docker compose down; exit $$ret
