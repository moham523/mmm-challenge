-- Pass-through of raw_cpi, unchanged.
select
    month,
    cpi_value
from {{ source('raw', 'raw_cpi') }}
