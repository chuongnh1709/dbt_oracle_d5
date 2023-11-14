with source as (
      select * from {{ source('ldm_fin', 't1') }}
),
renamed as (
    select
        {{ adapter.quote("C1") }}

    from source
)
select * from renamed
  