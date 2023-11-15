-- config 
/*
, pre_hook="{{ truncate_table('ldm_fin', this.table) }}"

 , indexes=[
        {'columns': ['c1'], 'unique': True},
        {'columns': ['c1', 'c2']}
    ]
        , post_hook="{{ count_table() }}"

*/
{{ 
  config(
      materialized='incremental'
    , unique_key='c1'
    , tags=["test", "ldm_fin"]
    , parallel=2
    , post_hook=["{{ count_table() }}" ,"{{ count_table() }}"]

  ) 
}}

{% set n_101 = 101 %}
{% set n_99 = 99 %}

-- model query  
with init_ as 
(
  select /*+ parallel (2)*/ 
      {{ n_101 }} c1
    , sysdate c2  
    , {{ var('p_effective_date') }}  as last_day
  from dual
)
select c1, c2, last_day from init_ 
union all 
select {{ n_99 }}, c2, last_day from init_ 
{% if is_incremental() %}
  where 1=1
{% endif %}
