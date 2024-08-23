select
    catalog_name,
    schema_name,
    table_name,
    origin,
    materialization
from sdf.information_schema.tables
where 
    catalog_name = 'PURINA' -- Only look at purina tables
    and schema_name not like 'STAGING%' -- Ignore staging tables
    and schema_name <> 'SNAPSHOTS' -- Snapshot metadata not pulled in by sdf
    and schema_name <> 'CLOUD_REPORTING' -- Ignore insights tables
    and origin <> 'remote' -- Ignore remote tables
    and materialization <> 'external-table' -- Ignore external tables
    and (description is null or trim(description) = '') -- Find all tables without descriptions
order by schema_name, table_name
