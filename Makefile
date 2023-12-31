dev_install:
	pip install -e ".[dev]"
	cd dbt && dbt deps && cd ..

manifest:
	cd dbt && dbt parse && cd ..

dev:
	make manifest
	dagster dev

lint:
	sqlfluff lint --config .sqlfluff ./dbt/models

fix:
	sqlfluff fix --config .sqlfluff ./dbt/models