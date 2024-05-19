from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago


from weather_pipeline import (
    extract_weather_data,
    transform_data,
    load_data_to_sqlite,
)


default_args = {
    "owner": "Irene",
    "start_date": days_ago(0),
    "email": ["i.busah@alustudent.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# initiating the dag
dag = DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="This is a simple ETL pipeline that retrieves data from a weather database",
    schedule_interval=timedelta(days=1),
)


# defining the tasks
extract_task = PythonOperator(
    task_id="extract_weather_data", python_callable=extract_weather_data, dag=dag
)


transform_task = PythonOperator(
    task_id="transform_weather_data", python_callable=transform_data, dag=dag
)


load_task = PythonOperator(
    task_id="load_weather_data", python_callable=load_data_to_sqlite, dag=dag
)


# defining the pipeline dependencies
extract_task >> transform_task >> load_task
