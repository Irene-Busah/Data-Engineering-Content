from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import timedelta
from airflow.utils.dates import days_ago  # type: ignore


# defining the default args
default_args = {
    "owner": "Irene",
    "start_date": days_ago(0),
    "email": ["i.busah@alustudent.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# defining a DAG
dag = DAG(
    dag_id="simple-etl-dag",
    default_args=default_args,
    description="Simple ETL DAG using Bash & Python",
    schedule_interval=timedelta(days=1),
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
