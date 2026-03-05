.PHONY: clean clean-postgresql clean-sqlite clean-mysql \
        generate generate-postgresql generate-sqlite generate-mysql \
        run run-postgresql run-sqlite run-mysql

clean-postgresql:
	$(MAKE) -C postgresql clean

clean-sqlite:
	$(MAKE) -C sqlite clean

clean-mysql:
	$(MAKE) -C mysql clean

clean: clean-postgresql clean-sqlite clean-mysql

generate-postgresql:
	$(MAKE) -C postgresql generate

generate-sqlite:
	$(MAKE) -C sqlite generate

generate-mysql:
	$(MAKE) -C mysql generate

generate: generate-postgresql generate-sqlite generate-mysql

run-postgresql:
	$(MAKE) -C postgresql run

run-sqlite:
	$(MAKE) -C sqlite run

run-mysql:
	$(MAKE) -C mysql run

run: run-postgresql run-sqlite run-mysql
