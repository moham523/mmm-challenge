-- Pass-through of raw_spend; week_end_date is re-derived to guarantee it's a Sunday.
select
    date(week_end_date, 'weekday 0') as week_end_date,
    channel,
    subchannel,
    spend,
    impressions
from {{ source('raw', 'raw_spend') }}
