-- Save model as 'dbt_results.sql'

{{
  config(
    materialized = 'incremental',
    transient = False,
    unique_key = 'result_id'
  )
}}

with empty_table as (
    select
        null as result_id,
        null as invocation_id,
        null as unique_id,
        null as database_name,
        null as schema_name,
        null as name,
        null as resource_type,
        null as status,
        cast(null as float) as execution_time,
        cast(null as int) as rows_affected
)

select * from empty_table
-- This is a filter so we will never actually insert these values
where 1 = 0

-- dbt run -m dbt_results

---------------------------------------------------------------------------
-- Manual Create table For Oracle

-- drop table ldm_sbv.dbt_results  ;

create table ldm_sbv.dbt_results  (
  log_date        date default CURRENT_DATE,
  started_at        varchar2(50),
  result_id       varchar2(200),
  invocation_id   varchar2(200),
  unique_id       varchar2(200),
  database_name   varchar2(200),
  schema_name     varchar2(200),
  name            varchar2(200),
  resource_type   varchar2(200),
  status          varchar2(200),
  execution_time  number(16,4),
  rows_affected   integer,
  message         varchar2(1000)
);

-- partition on log_date...

create unique index ldm_sbv.dbt_results_uniq on ldm_sbv.dbt_results(result_id);

/* -- get 2 first lines
with t1 as (
select 
'Database Error in model dbt_dc_client (models\marts\ldm_sbv_out\dbt_dc_client.sql)
  ORA-00904: "ABC": invalid identifier
  Help: https://docs.oracle.com/error-help/db/ora-00904/
  compiled Code at target\run\dbt_oracle_d5\models\marts\ldm_sbv_out\dbt_dc_client.sql' 
  as text 
from dual
) 
select
 REGEXP_SUBSTR (text, '.*$', 1, 1, 'm') || CHR (10) || REGEXP_SUBSTR (text, '.*$', 1, 3, 'm') AS first_two_lines
from t1
;
*/

select 
  LOG_DATE
  ,DATABASE_NAME
  ,SCHEMA_NAME
  ,NAME
  ,RESOURCE_TYPE
  ,STATUS
  ,EXECUTION_TIME
  ,ROWS_AFFECTED
  ,length(MESSAGE) message_length
  --, SUBSTR(MESSAGE,1,LEAST(100,LENGTH(MESSAGE)))
  ,MESSAGE
from ldm_sbv.dbt_results 
order by log_date desc ;