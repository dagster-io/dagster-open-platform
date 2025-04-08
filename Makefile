uv_install:
	pip install uv

uv_venv:
	if [ ! -d ".venv" ]; then uv venv; fi

test_install: uv_venv
	uv sync --extra tests

dev_install: uv_install uv_venv
	uv sync --extra dev --prerelease=allow
	cd dagster_open_platform_dbt && uv run dbt deps && cd ..

test: test_install
	uv run pytest dagster_open_platform_tests -m "not env_bk"  --disable-warnings

test_full: test_install
	@DOP_PYTEST_FULL=1 uv run pytest dagster_open_platform_tests -m "not env_bk"  --disable-warnings

update_snapshot: test_install
	@DOP_PYTEST_FULL=1 uv run pytest --snapshot-update dagster_open_platform_tests -k "snapshot" --disable-warnings

manifest: uv_venv
	cd dagster_open_platform_dbt && uv run dbt parse && cd ..

dev:
	uv run dg dev

lint:
	sqlfluff lint ./dagster_open_platform_dbt/models --disable-progress-bar --processes 4

fix:
	sqlfluff fix ./dagster_open_platform_dbt/models --disable-progress-bar --processes 4

install_ruff:
	$(MAKE) -C ../.. install_ruff

ruff:
	$(MAKE) -C ../.. ruff
