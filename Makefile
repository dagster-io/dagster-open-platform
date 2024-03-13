uv_install:
	pip install uv

test_install: uv_install
	uv pip install -e ".[tests]"

dev_install: uv_install
	uv pip install -e ".[dev]"
	cd dbt && dbt deps && cd ..

test: test_install
	pytest dagster_open_platform_tests

manifest:
	cd dbt && dbt parse && cd ..

dev:
	make manifest
	dagster dev

lint:
	sqlfluff lint --config .sqlfluff ./dbt/models

fix:
	sqlfluff fix --config .sqlfluff ./dbt/models
