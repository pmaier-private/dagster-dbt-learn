/*
One row per call session for aggregate-friendly analytics.
*/

select
    call_id,
    agent_name,
    agent_company,
    caller_name,
    caller_company,
    started_at,
    ended_at,
    duration_seconds,
    is_hangup
from {{ ref('int_call_sessions') }}