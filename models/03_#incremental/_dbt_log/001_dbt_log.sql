-- drop table ldm_sbv.dbt_log ;

create table ldm_sbv.dbt_run_log 
    (
        insert_time     date default CURRENT_TIMESTAMP,
        model_schema    varchar2(20),
        model_name      varchar2(200),
        model_type      varchar2(20),
        run_status      varchar2(50)
        )
;

select * 
from ldm_sbv.dbt_log 
order by insert_date desc ;