{{ config(materialized='ephemeral') }}
-- there will be no view or table created on database 

select * from  {{ ref('fin__test') }}
