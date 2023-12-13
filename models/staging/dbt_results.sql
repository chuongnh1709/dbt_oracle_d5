-- Save model as 'dbt_results.sql'

{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'result_id'
  )
}}

select 
    result_id
  , invocation_id 
  , unique_id
  , database_name
  , schema_name
  , name
  , resource_type 
  , status
  , execution_time
  , rows_affected
  , message
from dbt_results
-- This is a filter so we will never actually insert these values
where 1 = 0