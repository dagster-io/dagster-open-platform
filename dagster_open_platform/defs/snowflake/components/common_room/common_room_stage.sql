CREATE STAGE {{stage_name}}
URL='s3://common-room-shared-bucket20250328005107552900000001/data/{{directory}}'
STORAGE_INTEGRATION = "common-room-shared-bucket20250328005107552900000001"
DIRECTORY = (ENABLE = TRUE)
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    FILE_EXTENSION = '.jsonl'
);
