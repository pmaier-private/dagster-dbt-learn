
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}


with ranked as (
  select
    *,
    row_number() over (
      partition by call_id, timestamp
      order by call_id, timestamp
    ) as rn
  from {{ source('raw_voicecalls', 'raw_table') }}
)
select *
from ranked
where rn = 1
