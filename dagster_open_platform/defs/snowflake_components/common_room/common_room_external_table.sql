CREATE OR REPLACE EXTERNAL TABLE {{table_name}}(
    FILENAME VARCHAR AS METADATA$FILENAME,
    REPLICATION_DATE DATE AS cast(
        split_part(METADATA$FILENAME, '/', 3) || '-' ||
        split_part(METADATA$FILENAME, '/', 4) || '-' ||
        split_part(METADATA$FILENAME, '/', 5)
        as date
    )
)
PARTITION BY (REPLICATION_DATE)
LOCATION = @{{stage_name}}
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    FILE_EXTENSION = '.jsonl'
)
PATTERN = '.*[.]jsonl'
AUTO_REFRESH = FALSE
REFRESH_ON_CREATE = TRUE
COMMENT = 'External table for stage {{stage_name}}';
