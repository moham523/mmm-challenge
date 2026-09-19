-- Collapses stg_transactions (one row per order) down to one row per week,
-- by summing revenue for every transaction that falls in the same week_end_date.
select
    week_end_date,
    sum(revenue) as revenue
from {{ ref('stg_transactions') }}
group by week_end_date
