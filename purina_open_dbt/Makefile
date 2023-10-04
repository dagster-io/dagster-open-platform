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

