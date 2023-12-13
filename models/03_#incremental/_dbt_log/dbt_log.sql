-- drop table ldm_sbv.dbt_log ;

create table ldm_sbv.dbt_log 
    (
        log_date      date     default CURRENT_TIMESTAMP,
        model_schema  varchar2(20),
        model_name    varchar2(200),
        model_status  varchar2(50)
        )
;

select * from ldm_sbv.dbt_log ;