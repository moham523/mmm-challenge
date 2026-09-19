-- Final MMM input mart: one row per week, combining revenue, spend by
-- channel/subchannel, and that week's monthly CPI value.
--
-- The week list is built from the union of both intermediate models (not
-- just one of them) so a week present in only revenue or only spend still
-- shows up here instead of silently disappearing.
with weeks as (
    select week_end_date from {{ ref('int_weekly_revenue') }}
    union
    select week_end_date from {{ ref('int_weekly_spend_pivot') }}
)

select
    weeks.week_end_date,
    -- coalesce to 0: a week with no matching revenue/spend row means "none happened", not "unknown"
    coalesce(rev.revenue, 0) as revenue,
    coalesce(spend.spend_paid_search_google, 0) as spend_paid_search_google,
    coalesce(spend.spend_paid_search_bing, 0) as spend_paid_search_bing,
    coalesce(spend.spend_social_facebook, 0) as spend_social_facebook,
    coalesce(spend.spend_social_instagram, 0) as spend_social_instagram,
    -- CPI is monthly, not weekly, so every week in a given month gets that month's value
    cpi.cpi_value
from weeks
left join {{ ref('int_weekly_revenue') }} as rev
    on rev.week_end_date = weeks.week_end_date
left join {{ ref('int_weekly_spend_pivot') }} as spend
    on spend.week_end_date = weeks.week_end_date
left join {{ ref('stg_cpi') }} as cpi
    on strftime('%Y-%m', weeks.week_end_date) = cpi.month
order by weeks.week_end_date
