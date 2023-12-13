from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.decorators import task

with DAG('LIB_DCT_CLIENT',
         start_date=datetime(2023,1,1),
         schedule_interval='@daily',
         catchup=False
) as dag:
    dim_client_map_sbv = OracleOperator(
        task_id='dim_client_map_sbv',
        oracle_conn_id='VN00D5HD',
        sql='begin ldm_sbv_etl.LIB_DCT_CLIENT.dim_client_map_sbv(-9999,trunc(sysdate)-1,NULL); end;'
    )

    dim_client_load_sbv = OracleOperator(
        task_id='dim_client_load_sbv',
        oracle_conn_id='VN00D5HD',
        sql='begin ldm_sbv_etl.LIB_DCT_CLIENT.dim_client_load_sbv(-9999,trunc(sysdate)-1,NULL); end;'
    )

    '''dim_client_late_map_sbv = OracleStoredProcedureOperator(
        task_id='dim_client_late_map_sbv',
        oracle_conn_id='VN00D5HD',
        procedure='ldm_sbv_etl.LIB_DCT_CLIENT.dim_client_late_map_sbv',
        parameters={
            'p_process_key': '123',
            'p_effective_date': 'trunc(sysdate) - 1',
            'p_data_type': 'NULL'
        }
    )'''
    dim_client_late_map_sbv = OracleOperator(
        task_id='dim_client_late_map_sbv',
        oracle_conn_id='VN00D5HD',
        sql='begin ldm_sbv_etl.LIB_DCT_CLIENT.dim_client_late_map_sbv(-9999,trunc(sysdate)-1,NULL); end;'
    )

    dim_client_map_sbv >> dim_client_load_sbv >> dim_client_late_map_sbv
