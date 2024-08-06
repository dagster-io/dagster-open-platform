select
    catalog_name,
    schema_name,
    table_name,
    origin,
    materialization
from sdf.information_schema.tables
where 
    catalog_name = 'PURINA'
    and schema_name not like 'STAGING%'
    and schema_name <> 'SNAPSHOTS'
    and origin <> 'remote'
    and materialization <> 'external-table'
    and description is null