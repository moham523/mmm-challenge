-- Pass-through of raw_transactions, minus customer_id (PII dropped here for good).
select
    date as transaction_date,
    date(date, 'weekday 0') as week_end_date,
    revenue,
    units
from {{ source('raw', 'raw_transactions') }}
