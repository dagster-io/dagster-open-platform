from collections.abc import Callable

from dagster import AutomationCondition, template_var


@template_var
def cron_tick_passed() -> Callable[[str], AutomationCondition]:
    return AutomationCondition.cron_tick_passed
