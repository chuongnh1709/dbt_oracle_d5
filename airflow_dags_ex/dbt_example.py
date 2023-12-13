import os
import json
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
import cx_Oracle


HOME = os.environ["HOME"] # retrieve the location of your home folslder
#LD_LIBRARY_PATH = os.environ["LD_LIBRARY_PATH"] # retrieve the location of your home folslder
#cx_Oracle.init_oracle_client(lib_dir=r"/usr/lib")

# Build an Airflow DAG
with DAG(
  dag_id="dbt_example", # The name that shows up in the UI
  start_date=pendulum.today(), # Start date of the DAG
  catchup=False,
) as dag:

  # Create a dict of Operators
  
      dbt_deps = BashOperator(
          task_id='dbt_deps',
          
          #bash_command=f"dbt debug", # run the model!
          bash_command=f"cd /usr/local/airflow/dbt" # Go to the path containing your dbt project
          + f" && python -m virtualenv .dbtenv && source .dbtenv/bin/activate"
          + f" && dbt deps", # run the model!
      )

      dct_client = BashOperator(
          task_id='dct_client',
          
          #bash_command=f"dbt debug", # run the model!
          bash_command=f"cd /usr/local/airflow/dbt" # Go to the path containing your dbt project
          + f" && python -m virtualenv .dbtenv && source .dbtenv/bin/activate"
          + f" && dbt build --model dbt_dct_client", # run the model!
      )

      test_dbt = BashOperator(
          task_id='test_dbt',
          
          #bash_command=f"dbt debug", # run the model!
          bash_command=f"cd /usr/local/airflow/dbt" # Go to the path containing your dbt project
          + f" && python -m virtualenv .dbtenv && source .dbtenv/bin/activate"
          + f" && dbt build --model test_tri", # run the model!
      )

  # Define relationships between Operators
dbt_deps>>test_dbt>>dct_client