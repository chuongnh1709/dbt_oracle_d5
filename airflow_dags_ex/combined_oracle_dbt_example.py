import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
import cx_Oracle

# Build an Airflow DAG
with DAG(
  dag_id="combined_oracle_dbt_example", # The name that shows up in the UI
  start_date=pendulum.today(), # Start date of the DAG
  catchup=False,
) as dag:
      truncate_table = OracleOperator(
          task_id='truncate_table',
          oracle_conn_id='VN00D5HD',
          sql="""BEGIN
                    EXECUTE IMMEDIATE 'truncate table ldm_sbv.dbt_dct_client';
                 END;"""
      )

      insert_dct_client = BashOperator(
          task_id='insert_dct_client',
          bash_command=f"cd /usr/local/airflow/dbt" # Go to the path containing your dbt project
          + f" && python -m virtualenv .dbtenv && source .dbtenv/bin/activate"
          + f" && dbt build --model dbt_dct_client",
      )

  # Define relationships between Operators
truncate_table>>insert_dct_client
