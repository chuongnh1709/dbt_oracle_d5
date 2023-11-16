{{ 
  config(
      materialized='view'
    , pre_hook="{{ truncate_table_v2(this.schema, this.table~'_test') }}"
    , post_hook=["{{ count_table() }}", "{{fn_verifica_id(2) }}"]
  ) 
}}


{% set n_101 = 101 %}

  select 
      {{ n_101 }} c1
    , sysdate c2  
    , {{ var('p_effective_date') }}  as last_day
  from dual
