
select
    table_id,
    column_name
from sdf.information_schema.columns
where 
    catalog_name = 'DWH_REPORTING' -- Only look at dwh_reporting tables
    and (description is null or trim(description) = '')
