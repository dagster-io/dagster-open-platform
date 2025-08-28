from dagster import AssetSelection, ScheduleDefinition

# Note: Assets are imported in definitions.py, not here to avoid circular imports

sequel_events_schedule = ScheduleDefinition(
    name="sequel_events_schedule",
    cron_schedule="0 */6 * * *",  # Run every 6 hours
    target=AssetSelection.assets(
        ("sequel", "full_refresh", "events")
    ),  # Use composite key as tuple
    description="Schedule that processes Sequel events data since last sync every 6 hours",
)


sequel_registrants_schedule = ScheduleDefinition(
    name="sequel_registrants_schedule",
    cron_schedule="30 */6 * * *",  # Run every 6 hours, 30 minutes after events
    target=AssetSelection.assets(
        ("sequel", "full_refresh", "registrants")
    ),  # Use composite key as tuple
    description="Schedule that processes Sequel registrants data since last sync every 6 hours",
)


sequel_event_logs_schedule = ScheduleDefinition(
    name="sequel_event_logs_schedule",
    cron_schedule="0 2 * * 0",  # Run once weekly on Sunday at 2 AM
    target=AssetSelection.assets(
        ("sequel", "full_refresh", "user_activity")
    ),  # Use composite key as tuple
    description="Weekly full refresh of Sequel event user activity logs for data consistency",
)
