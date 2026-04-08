/*
Pivot events into sessions, introducing session grain.
*/

with events as (
    select * from {{ ref('stg_deduplicate_raw') }}
),
pivoted as (
    select
        call_id,
        max(
            case
                when event_type = 'call_initiated' then timestamp
            end
        ) as started_at,
        max(
            case
                when event_type = 'call_ended' then timestamp
            end
        ) as ended_at,
        max(
            case
                when event_type = 'call_initiated' then name
            end
        ) as agent_name,
        max(
            case
                when event_type = 'call_initiated' then company
            end
        ) as agent_company,
        max(
            case
                when event_type = 'call_answered' then name
            end
        ) as caller_name,
        max(
            case
                when event_type = 'call_answered' then company
            end
        ) as caller_company
    from
        events
    group by
        call_id
)
select
    call_id,
    agent_name,
    agent_company,
    caller_name,
    caller_company,
    started_at,
    ended_at,
    extract(
        epoch
        from
            (ended_at - started_at)
    ) as duration_seconds,
    extract(
        epoch
        from
            (ended_at - started_at)
    ) < 15 as is_hangup
from
    pivoted