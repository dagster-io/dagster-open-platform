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

check-lineage:
	dbt compile
	sdf dbt refresh --no-save
	sdf compile