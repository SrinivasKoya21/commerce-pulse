# Convenience targets — `make test` before every commit.
.PHONY: test lint produce dbt-build

test:
	pytest databricks/tests -q

lint:
	ruff check producers databricks/src databricks/tests

produce:
	cd producers && python producer_kafka.py --eps 20 --duration 900

dbt-build:
	cd dbt/commerce_dbt && dbt build
