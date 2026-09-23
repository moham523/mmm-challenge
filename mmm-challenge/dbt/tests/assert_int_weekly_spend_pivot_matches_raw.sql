-- Singular dbt test: recomputes weekly spend per channel/subchannel straight
-- from raw_spend, independent of int_weekly_spend_pivot's own pivot logic,
-- and fails if any of the 4 hardcoded columns disagrees with the model.
with recomputed as (
    select
        week_end_date,
        channel,
        subchannel,
        sum(spend) as spend
    from {{ source('raw', 'raw_spend') }}
    group by week_end_date, channel, subchannel
),

model_unpivoted as (
    select week_end_date, 'Paid Search' as channel, 'Google' as subchannel, spend_paid_search_google as model_spend
    from {{ ref('int_weekly_spend_pivot') }}
    union all
    select week_end_date, 'Paid Search', 'Bing', spend_paid_search_bing
    from {{ ref('int_weekly_spend_pivot') }}
    union all
    select week_end_date, 'Social', 'Facebook', spend_social_facebook
    from {{ ref('int_weekly_spend_pivot') }}
    union all
    select week_end_date, 'Social', 'Instagram', spend_social_instagram
    from {{ ref('int_weekly_spend_pivot') }}
)

select
    model_unpivoted.week_end_date,
    model_unpivoted.channel,
    model_unpivoted.subchannel,
    model_unpivoted.model_spend,
    recomputed.spend as recomputed_spend
from model_unpivoted
join recomputed
    on recomputed.week_end_date = model_unpivoted.week_end_date
    and recomputed.channel = model_unpivoted.channel
    and recomputed.subchannel = model_unpivoted.subchannel
where model_unpivoted.model_spend != recomputed.spend
