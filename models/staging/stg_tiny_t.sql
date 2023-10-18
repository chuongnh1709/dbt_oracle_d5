with source as (
      select 
          id
        , status
        , updated_at
      from {{ source('bq_source', 'tiny_t') }}  --source
),
renamed as (
    select
          id          as skp_id
        , status      as code_status 
        , updated_at 
    from source
)
select * from renamed
