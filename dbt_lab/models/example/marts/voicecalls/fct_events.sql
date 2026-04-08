
-- Use the `ref` function to select from other models

select
    timestamp,
    call_id,
    name,
    event_type,
    transcript,
    company
from {{ ref('stg_deduplicate_raw') }}
where event_type = 'call_initiated'
