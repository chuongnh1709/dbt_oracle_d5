{{ 
  config(
    materialized='view'
    ,enabled=false
  ) 
}}

{# -- sql 
{{ dbt_utils.deduplicate( -- Not work with Oracle cause syntax 
    relation=ref('fin__test'),
    partition_by='c1',
    order_by='c2 desc',
   )
}}

#}

select c1, c2, last_day , wrong_col
from {{ ref('fin__test')}} 