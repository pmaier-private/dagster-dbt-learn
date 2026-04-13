
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}
-- depends_on: {{ source('dagster', 'raw_table') }}

with source_data as (
  select *
  from {{ source('raw_voicecalls', 'raw_table') }}
),

schema_enforced as (
  select
    cast(call_id as text) as call_id,
    cast(event_type as text) as event_type,
    cast(nullif("timestamp", '') as timestamp) as "timestamp",
    cast(name as text) as name,
    cast(company as text) as company,
    cast(transcript as text) as transcript
  from
    source_data
),
ranked as (
  select
    *,
    row_number() over (
      partition by
        call_id,
        "timestamp"
      order by
        call_id,
        "timestamp"
    ) as rn
  from
    schema_enforced
)
select
  *
from
  ranked
where
  rn = 1

