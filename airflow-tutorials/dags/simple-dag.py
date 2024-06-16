from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import timedelta
from airflow.utils.dates import days_ago
from datetime import datetime


# defining the default args
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 6, 16),
}

# defining a DAG
dag = DAG(
    dag_id="simple-etl-dag",
    default_args=default_args,
    description="Simple ETL DAG using Bash & Python",
    catchup=False,
)

# defining the tasks
extract = BashOperator(
    task_id="extract",
    bash_command="echo 'Extracting Data from different sources'",
    dag=dag,
)

transform = BashOperator(
    task_id="transform",
    bash_command="echo 'Transforming the extracted data by filling the missing values'",
    dag=dag,
)

load = BashOperator(
    task_id="load",
    bash_command="echo 'Loading the transformed data in to SQLite database'",
    dag=dag,
)


# task pipeline
extract >> transform >> load
