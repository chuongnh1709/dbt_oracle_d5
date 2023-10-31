{{ 
  config(
    materialized='incremental'
    , unique_key='c1'
    , tags=["test", "ldm_fin"]
    , pre_hook="{{ truncate_table('ldm_fin', this.table) }}"
  ) 
}}

-- sql 
select 123 c1, sysdate c2  from dual

{% if is_incremental() %}
  where 1=1
{% endif %}