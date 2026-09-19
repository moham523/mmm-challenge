-- Pivots stg_spend from "one row per week/channel/subchannel" into "one row
-- per week", with each channel/subchannel combo becoming its own column.
-- The 4 combos are hardcoded since the source data only ever has these 4
-- (confirmed by inspecting spend.csv) -- a new channel would need a new
-- column added here.
select
    week_end_date,
    sum(case when channel = 'Paid Search' and subchannel = 'Google' then spend else 0 end) as spend_paid_search_google,
    sum(case when channel = 'Paid Search' and subchannel = 'Bing' then spend else 0 end) as spend_paid_search_bing,
    sum(case when channel = 'Social' and subchannel = 'Facebook' then spend else 0 end) as spend_social_facebook,
    sum(case when channel = 'Social' and subchannel = 'Instagram' then spend else 0 end) as spend_social_instagram
from {{ ref('stg_spend') }}
group by week_end_date
