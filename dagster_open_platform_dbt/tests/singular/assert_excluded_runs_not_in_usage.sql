-- Warns if an excluded run still has rows in the step or usage models.
-- fct_steps is incremental, so rows loaded before a run was excluded stay
-- until fct_steps and base_step_metrics are rebuilt for those dates.
{{ config(severity="warn") }}

with excluded as (
    select run_id from {{ ref('run_exclusions') }}
)

select 'fct_steps' as model, fct_steps.run_id
from {{ ref('fct_steps') }} as fct_steps
inner join excluded on fct_steps.run_id = excluded.run_id

union all

select 'usage_metrics_daily' as model, usage_daily.run_id
from {{ ref('usage_metrics_daily') }} as usage_daily
inner join excluded on usage_daily.run_id = excluded.run_id
