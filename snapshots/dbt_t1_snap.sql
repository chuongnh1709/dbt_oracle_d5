{#
    This is model SCD-2 snapshot for source ldm_sbv.dbt_t1_src table
    Not support for first run on Oracle yet 
      - snapshot table have to create first 
      - sub-sequence running also have some syntax error for Oracle 
#}

{% snapshot dbt_t1_snap %}

{{
    config(
      target_schema='ldm_sbv',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select * from {{ source('ldm_sbv', 'dbt_t1_src') }}

{% endsnapshot %}
