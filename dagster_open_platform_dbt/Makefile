run:
	dbt run -x

build:
	dbt build -x

test:
	dbt test

run-non-cloud:
	dbt run --selector non_cloud_analytics

full-run-non-cloud:
	dbt run --selector non_cloud_analytics --vars 'ignore_date_limits: true'

install-sdf:
	curl -LSfs https://cdn.sdf.com/releases/download/install.sh | bash -s --

update-sdf:
	curl -LSfs https://cdn.sdf.com/releases/download/install.sh | bash -s -- --update

auth-sdf:
	sdf auth login snowflake \
		--account-id=${SNOWFLAKE_ACCOUNT} \
		--username=${SNOWFLAKE_USER} \
		--password=${SNOWFLAKE_PASSWORD} \
		--warehouse=${SNOWFLAKE_WAREHOUSE} \
		--role=${SNOWFLAKE_ROLE}

check-lineage: auth-sdf
	dbt deps
	dbt compile --target sdf
	sdf dbt refresh
	sdf compile

SELECT ?= ""
run-local-read-prod:
	dbt compile --target=prod
	mv target prod-target
	dbt run --select $(SELECT) --defer --state prod-target
	rm -rf prod-target

build-prod-manifest:
	dbt parse --target sdf --target-path target_prod

update-dbt-project:
	git pull
	make build-prod-manifest

# Bring in any changes from master and submit the branch to PR
get-latest-and-submit:
	@if [ "$$(git rev-parse --abbrev-ref HEAD)" = "master" ]; then \
		echo "Cannot push to master! Please create a branch."; \
		exit 1; \
	else \
		gt get && gt submit; \
	fi