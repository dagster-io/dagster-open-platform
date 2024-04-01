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
	sqlfluff lint ./dbt/models --disable-progress-bar --processes 4

fix:
	sqlfluff fix ./dbt/models --disable-progress-bar --processes 4
