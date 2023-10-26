manifest:
	cd dbt
	dbt deps
	dbt parse
	cd ..
