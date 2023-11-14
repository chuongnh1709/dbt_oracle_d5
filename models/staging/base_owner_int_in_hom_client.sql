{{ 
  config(
    materialized='view'
  ) 
}}
    select
        {{ adapter.quote("CODE_SOURCE_SYSTEM") }},
        {{ adapter.quote("DATE_EFFECTIVE_INSERTED") }},
        {{ adapter.quote("ID") }},
        {{ adapter.quote("VERSION") }},
        {{ adapter.quote("CUID") }},
        {{ adapter.quote("CREATION_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("CODE_CHANGE_TYPE") }},
        {{ adapter.quote("CODE_LOAD_STATUS") }},
        {{ adapter.quote("DESC_LOAD_ERROR") }},
        {{ adapter.quote("DTIME_INSERTED") }},
        {{ adapter.quote("SK_PROC_INSERTED") }}

    from {{ source('owner_int', 'in_hom_client') }}
