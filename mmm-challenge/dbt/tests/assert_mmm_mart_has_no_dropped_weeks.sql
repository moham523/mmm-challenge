-- Singular dbt test: fails if any week present in either intermediate model
-- is missing from mmm_mart. A dbt test "fails" when its query returns rows,
-- so this returns the missing week(s) if the mart's join silently dropped one.
with expected_weeks as (
    select week_end_date from {{ ref('int_weekly_revenue') }}
    union
    select week_end_date from {{ ref('int_weekly_spend_pivot') }}
)

select expected_weeks.week_end_date
from expected_weeks
left join {{ ref('mmm_mart') }} as mart
    on mart.week_end_date = expected_weeks.week_end_date
where mart.week_end_date is null
