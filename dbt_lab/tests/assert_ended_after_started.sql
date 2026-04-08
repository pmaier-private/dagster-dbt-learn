select * from {{ ref('fct_calls') }}
where ended_at < started_at