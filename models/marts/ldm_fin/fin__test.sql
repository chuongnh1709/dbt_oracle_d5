{{ 
  config(
    materialized='incremental'
    , unique_key='c1'
    , tags=["test", "ldm_fin"]
    , pre_hook="{{ truncate_table('ldm_fin', this.table) }}"
    , parallel=4 
  ) 
}}

-- sql 
select /*+ parallel (2)*/ 123 c1, sysdate c2  from dual

{% if is_incremental() %}
  where 1=1
{% endif %}
