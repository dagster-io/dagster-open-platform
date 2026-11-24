-- Fails if a run listed in run_exclusions is still in fct_runs.

select fct_runs.run_id
from {{ ref('fct_runs') }} as fct_runs
inner join {{ ref('run_exclusions') }} as excluded
    on fct_runs.run_id = excluded.run_id
