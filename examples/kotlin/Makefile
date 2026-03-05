.PHONY: clean clean-postgresql clean-sqlite clean-mysql \
        run run-postgresql run-sqlite run-mysql

clean-postgresql:
	$(MAKE) -C postgresql clean

clean-sqlite:
	$(MAKE) -C sqlite clean

clean-mysql:
	$(MAKE) -C mysql clean

clean: clean-postgresql clean-sqlite clean-mysql

run-postgresql:
	$(MAKE) -C postgresql run

run-sqlite:
	$(MAKE) -C sqlite run

run-mysql:
	$(MAKE) -C mysql run

run: run-postgresql run-sqlite run-mysql
