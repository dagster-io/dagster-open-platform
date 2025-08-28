from dagster import AssetSelection, ScheduleDefinition

# Note: Assets are imported in definitions.py, not here to avoid circular imports

sequel_events_schedule = ScheduleDefinition(
    name="sequel_events_schedule",
    cron_schedule="0 1 * * *",  # Run once daily at 1 AM EST
    target=AssetSelection.assets(
        ("sequel", "full_refresh", "events")
    ),  # Use composite key as tuple
    description="Schedule that processes Sequel events data once daily at 1 AM EST",
)


sequel_registrants_schedule = ScheduleDefinition(
    name="sequel_registrants_schedule",
    cron_schedule="15 1 * * *",  # Run once daily at 1:15 AM EST
    target=AssetSelection.assets(
        ("sequel", "full_refresh", "registrants")
    ),  # Use composite key as tuple
    description="Schedule that processes Sequel registrants data once daily at 1:15 AM EST",
)


sequel_user_activity_logs_schedule = ScheduleDefinition(
    name="sequel_user_activity_logs_schedule",
    cron_schedule="30 1 * * *",  # Run once daily at 1:30 AM EST
    target=AssetSelection.assets(
        ("sequel", "full_refresh", "user_activity")
    ),  # Use composite key as tuple
    description="Daily processing of Sequel event user activity logs at 1:30 AM EST",
)
