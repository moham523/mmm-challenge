-- Singular dbt test: recomputes weekly revenue straight from raw_transactions,
-- independent of int_weekly_revenue's own logic, and fails if any week
-- disagrees with what the model produced.
with recomputed as (
    select
        date(date, 'weekday 0') as week_end_date,
        sum(revenue) as revenue
    from {{ source('raw', 'raw_transactions') }}
    group by week_end_date
)

select
    model.week_end_date,
    model.revenue as model_revenue,
    recomputed.revenue as recomputed_revenue
from {{ ref('int_weekly_revenue') }} as model
join recomputed on recomputed.week_end_date = model.week_end_date
where model.revenue != recomputed.revenue
