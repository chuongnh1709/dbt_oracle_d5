from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

table_name = "LDM_SBV.CLT_SBV_REPORT_RISK_GROUP"

@task
def get_data_from_oracle():
    oracle_hook = OracleHook(oracle_conn_id='oracle_default')
    data = oracle_hook.get_pandas_df(sql=f"SELECT code_sbv_report_risk_group,desc_sbv_report_risk_group FROM {table_name}")
    return data.to_dict()

@task
def insert_data_into_postgres(data):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_hook.insert_rows(table=f"CLT_SBV_REPORT_RISK_GROUP",rows=data)

with DAG('oracle_to_postgres',start_date=datetime(2022,1,1)) as dag:
    data = get_data_from_oracle()
    insert_data_into_postgres(data)