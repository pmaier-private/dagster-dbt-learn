select * from {{ ref('fct_calls') }}
where duration_seconds <= 0