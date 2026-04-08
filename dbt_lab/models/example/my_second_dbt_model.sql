
-- Use the `ref` function to select from other models

select *
from {{ ref('deduplicate_raw') }}
where event_type = 'call_initiated'
