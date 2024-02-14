from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datasource import send_data

start_date = datetime(2018, 12, 21, 12, 12)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('user_data', default_args=default_args, schedule_interval='*/5 * * * *', catchup=False) as dag:


    data_stream_task = PythonOperator(
    task_id='kafka_data_stream',
    python_callable=send_data,
    dag=dag,
    )

    data_stream_task # type: ignore
